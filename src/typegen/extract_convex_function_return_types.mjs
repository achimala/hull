import path from "node:path";
import process from "node:process";
import { createRequire } from "node:module";
import fs from "node:fs";

function parseArgs(argv) {
  const out = new Map();
  for (let i = 1; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg.startsWith("--")) {
      continue;
    }
    const key = arg.slice(2);
    const value = argv[i + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`Missing value for --${key}`);
    }
    out.set(key, value);
    i += 1;
  }
  return out;
}

function unwrapAwaitedType(checker, type) {
  // TypeScript public APIs differ slightly across versions; try both.
  if (typeof checker.getAwaitedType === "function") {
    const awaited = checker.getAwaitedType(type);
    if (awaited) return awaited;
  }
  if (typeof checker.getPromisedTypeOfPromise === "function") {
    const promised = checker.getPromisedTypeOfPromise(type);
    if (promised) return promised;
  }
  return type;
}

function isCallToConvexBuilder(ts, node) {
  if (!ts.isCallExpression(node)) return null;
  const callee = node.expression;
  if (!ts.isIdentifier(callee)) return null;
  switch (callee.text) {
    case "query":
    case "internalQuery":
    case "mutation":
    case "internalMutation":
    case "action":
    case "internalAction":
    case "httpAction":
      return callee.text;
    default:
      return null;
  }
}

function getHandlerFunction(ts, callExpr, builderKind) {
  if (builderKind === "httpAction") {
    if (callExpr.arguments.length < 1) return null;
    return callExpr.arguments[0];
  }

  if (callExpr.arguments.length < 1) return null;
  const arg0 = callExpr.arguments[0];
  if (!ts.isObjectLiteralExpression(arg0)) return null;
  for (const prop of arg0.properties) {
    if (!ts.isPropertyAssignment(prop)) continue;
    const name = prop.name;
    if (
      (ts.isIdentifier(name) && name.text === "handler") ||
      (ts.isStringLiteral(name) && name.text === "handler")
    ) {
      return prop.initializer;
    }
  }
  return null;
}

function stringLiteralTypeValue(ts, type) {
  if ((type.flags & ts.TypeFlags.StringLiteral) === 0) return null;
  return type.value;
}

function stripUndefinedFromRepr(repr) {
  if (!repr || typeof repr !== "object") return repr;
  if (repr.type !== "union") return repr;
  const members = (repr.members ?? []).filter((member) => member.type !== "undefined");
  if (members.length === 0) return { type: "any" };
  if (members.length === 1) return members[0];
  return { type: "union", members };
}

function serializeType(ts, checker, type, seen, fallbackNode) {
  // Prevent infinite recursion on self-referential types.
  if (seen.includes(type)) {
    return { type: "any" };
  }
  const nextSeen = [...seen, type];

  const typeString = checker.typeToString(type);
  const docMatch = /(?:^|\b)Doc<"([^"]+)">$/.exec(typeString);
  if (docMatch) {
    return { type: "doc", table: docMatch[1] };
  }
  const idMatch = /(?:^|\b)Id<"([^"]+)">$/.exec(typeString);
  if (idMatch) {
    return { type: "id", table: idMatch[1] };
  }

  if (type.isUnion()) {
    return {
      type: "union",
      members: type.types.map((member) =>
        serializeType(ts, checker, member, nextSeen, fallbackNode)
      ),
    };
  }

  if (type.flags & ts.TypeFlags.Null) return { type: "null" };
  if (type.flags & ts.TypeFlags.Undefined) return { type: "undefined" };
  if (type.flags & ts.TypeFlags.Void) return { type: "undefined" };

  if (type.flags & ts.TypeFlags.Any) return { type: "any" };
  if (type.flags & ts.TypeFlags.Unknown) return { type: "any" };

  if (type.flags & ts.TypeFlags.String) return { type: "string" };
  if (type.flags & ts.TypeFlags.Number) return { type: "number" };
  if (type.flags & ts.TypeFlags.Boolean) return { type: "boolean" };
  if (type.flags & ts.TypeFlags.BooleanLiteral) return { type: "boolean" };
  if (type.flags & ts.TypeFlags.BigInt) return { type: "int64" };

  if (type.flags & ts.TypeFlags.StringLiteral) {
    return { type: "literal_string", value: type.value };
  }
  if (type.flags & ts.TypeFlags.NumberLiteral) {
    return { type: "literal_number", value: type.value };
  }

  if ((type.flags & ts.TypeFlags.Object) !== 0) {
    const objectType = type;

    const typeString = checker.typeToString(type);
    const docMatch = /(?:^|\b)Doc<"([^"]+)">$/.exec(typeString);
    if (docMatch) {
      return { type: "doc", table: docMatch[1] };
    }
    const idMatch = /(?:^|\b)Id<"([^"]+)">$/.exec(typeString);
    if (idMatch) {
      return { type: "id", table: idMatch[1] };
    }

    // Doc types can expand structurally; detect them via the `_id` + `_creationTime` fields.
    // This avoids emitting enormous inline object return types for basic `db.get`/`query` flows.
    const idProp = typeof type.getProperty === "function" ? type.getProperty("_id") : null;
    const creationTimeProp =
      typeof type.getProperty === "function" ? type.getProperty("_creationTime") : null;
    if (idProp && creationTimeProp) {
      const idLocation = idProp.valueDeclaration ?? idProp.declarations?.[0] ?? fallbackNode;
      const ctLocation =
        creationTimeProp.valueDeclaration ??
        creationTimeProp.declarations?.[0] ??
        fallbackNode;
      if (idLocation && ctLocation) {
        const idType = checker.getTypeOfSymbolAtLocation(idProp, idLocation);
        const ctType = checker.getTypeOfSymbolAtLocation(creationTimeProp, ctLocation);
        const idMatch = /(?:^|\b)Id<"([^"]+)">$/.exec(checker.typeToString(idType));
        if (idMatch && (ctType.flags & ts.TypeFlags.Number) !== 0) {
          return { type: "doc", table: idMatch[1] };
        }
      }
    }

    // Doc<"table"> and Id<"table">
    if (
      (objectType.objectFlags & ts.ObjectFlags.Reference) !== 0 &&
      objectType.target &&
      objectType.target.symbol
    ) {
      const symbolName = objectType.target.symbol.name;
      const args = objectType.typeArguments ?? [];
      if ((symbolName === "Doc" || symbolName === "Id") && args.length === 1) {
        const tableName = stringLiteralTypeValue(ts, args[0]);
        if (tableName) {
          return {
            type: symbolName === "Doc" ? "doc" : "id",
            table: tableName,
          };
        }
      }
    }

    if (typeof checker.isArrayType === "function" && checker.isArrayType(type)) {
      const elementType = checker.getElementTypeOfArrayType(type);
      if (!elementType) {
        throw new Error(`Array element type missing: ${checker.typeToString(type)}`);
      }
      return {
        type: "array",
        items: serializeType(ts, checker, elementType, nextSeen, fallbackNode),
      };
    }

    const indexType =
      typeof type.getStringIndexType === "function" ? type.getStringIndexType() : null;
    if (indexType) {
      return {
        type: "record",
        key: { type: "string" },
        value: serializeType(ts, checker, indexType, nextSeen, fallbackNode),
      };
    }

    const fields = {};
    for (const prop of type.getProperties()) {
      const decl = prop.valueDeclaration ?? prop.declarations?.[0];
      const location = decl ?? fallbackNode;
      if (!location) continue;
      const propType = checker.getTypeOfSymbolAtLocation(prop, location);
      const optional = (prop.flags & ts.SymbolFlags.Optional) !== 0;
      const serialized = serializeType(ts, checker, propType, nextSeen, fallbackNode);
      fields[prop.name] = {
        type: optional ? stripUndefinedFromRepr(serialized) : serialized,
        optional,
      };
    }
    return { type: "object", fields };
  }

  throw new Error(`Unsupported return type: ${checker.typeToString(type)}`);
}

async function main() {
  const args = parseArgs(process.argv);
  const convexDirArg = args.get("convex-dir");
  if (!convexDirArg) {
    throw new Error("Missing required --convex-dir");
  }
  const convexDir = path.resolve(convexDirArg);
  const convexPackageRoot = path.dirname(convexDir);
  const require = createRequire(path.join(convexPackageRoot, "package.json"));
  const ts = require("typescript");

  const tsconfigPath = path.join(convexDir, "tsconfig.json");
  const configFile = ts.readConfigFile(tsconfigPath, ts.sys.readFile);
  if (configFile.error) {
    throw new Error(ts.flattenDiagnosticMessageText(configFile.error.messageText, "\n"));
  }
  const config = ts.parseJsonConfigFileContent(
    configFile.config,
    ts.sys,
    convexDir
  );

  const generatedDir = path.join(convexDir, "_generated");
  const generatedDts = fs.existsSync(generatedDir)
    ? fs
        .readdirSync(generatedDir)
        .filter((file) => file.endsWith(".d.ts"))
        .map((file) => path.join(generatedDir, file))
    : [];

  const program = ts.createProgram({
    rootNames: [...config.fileNames, ...generatedDts],
    options: {
      ...config.options,
      // Convex ships typed `.d.ts` in `_generated/`; `allowJs` can cause TS to
      // resolve to the untyped `.js` siblings instead.
      allowJs: false,
      checkJs: false,
    },
  });
  const checker = program.getTypeChecker();

  const out = {};
  for (const sf of program.getSourceFiles()) {
    const filename = path.resolve(sf.fileName);
    if (!filename.startsWith(convexDir + path.sep)) {
      continue;
    }
    if (!filename.endsWith(".ts")) continue;
    if (filename.endsWith(".d.ts")) continue;

    const moduleName = path
      .relative(convexDir, filename)
      .replace(/\\/g, "/")
      .replace(/\.ts$/, "");
    if (moduleName === "schema" || moduleName === "validators" || moduleName === "auth.config") {
      continue;
    }

    for (const stmt of sf.statements) {
      if (!ts.isVariableStatement(stmt)) continue;
      const modifiers = ts.getCombinedModifierFlags(stmt);
      if ((modifiers & ts.ModifierFlags.Export) === 0) continue;

      for (const decl of stmt.declarationList.declarations) {
        if (!ts.isIdentifier(decl.name)) continue;
        if (!decl.initializer) continue;
        const builderKind = isCallToConvexBuilder(ts, decl.initializer);
        if (!builderKind) continue;
        const handlerExpr = getHandlerFunction(ts, decl.initializer, builderKind);
        if (!handlerExpr) {
          throw new Error(`${moduleName}:${decl.name.text} missing handler`);
        }

        const handlerType = checker.getTypeAtLocation(handlerExpr);
        const signatures = handlerType.getCallSignatures();
        if (signatures.length !== 1) {
          throw new Error(
            `${moduleName}:${decl.name.text} handler signatures=${signatures.length}`
          );
        }
        const returnType = unwrapAwaitedType(
          checker,
          checker.getReturnTypeOfSignature(signatures[0])
        );

        const key = `${moduleName}:${decl.name.text}`;
        try {
          if (builderKind === "httpAction") {
            // httpAction returns HTTP Response objects, not Convex JSON values.
            out[key] = { type: "any" };
          } else {
            out[key] = serializeType(ts, checker, returnType, [], sf);
          }
        } catch (err) {
          throw new Error(`${key}: ${String(err?.message ?? err)}`);
        }
      }
    }
  }

  process.stdout.write(JSON.stringify(out));
}

main().catch((err) => {
  process.stderr.write(String(err?.stack ?? err) + "\n");
  process.exit(1);
});
