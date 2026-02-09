use anyhow::{anyhow, Context, Result};
use indexmap::IndexMap;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use swc_common::{FileName, SourceMap};
use swc_ecma_ast::*;
use swc_ecma_parser::{Parser, StringInput, Syntax, TsSyntax};

mod naming;
mod rust_api;

#[cfg(test)]
mod tests;

use self::naming::*;
use self::rust_api::{generate_rust_api, RustFunctionDef};

#[derive(Clone, Debug)]
pub struct TypegenConfig {
    pub convex_dir: PathBuf,
    pub swift_out: PathBuf,
    pub rust_out: PathBuf,
    pub rust_api_out: PathBuf,
    pub rust_convex_value_path: String,
    pub rust_api_client_module_path: String,
    pub rust_api_types_module_path: String,
    pub include_internal_functions: bool,
}

pub fn run(config: TypegenConfig) -> Result<()> {
    run_with_return_type_extractor(config, extract_function_return_types)
}

fn sanitize_rust_use_path(path: &str, field_name: &str) -> Result<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{} cannot be empty", field_name));
    }
    if trimmed.contains('\n') || trimmed.contains('\r') || trimmed.contains(';') {
        return Err(anyhow!(
            "{} contains invalid characters for a Rust use path",
            field_name
        ));
    }
    Ok(trimmed.to_string())
}

fn run_with_return_type_extractor<F>(config: TypegenConfig, extract_return_types: F) -> Result<()>
where
    F: Fn(&Path, &HashMap<String, String>) -> Result<HashMap<String, TypeExpr>>,
{
    let rust_convex_value_path =
        sanitize_rust_use_path(&config.rust_convex_value_path, "rust_convex_value_path")?;
    let rust_api_client_module_path = sanitize_rust_use_path(
        &config.rust_api_client_module_path,
        "rust_api_client_module_path",
    )?;
    let rust_api_types_module_path = sanitize_rust_use_path(
        &config.rust_api_types_module_path,
        "rust_api_types_module_path",
    )?;

    let validators_path = config.convex_dir.join("validators.ts");
    let schema_path = config.convex_dir.join("schema.ts");

    let validators_module = parse_optional_ts_module(&validators_path)?;
    let schema_module = parse_ts_module(&schema_path)?;

    let validators_env = match validators_module {
        Some(module) => ModuleEnv::from_module(&module, &validators_path)?,
        None => ModuleEnv::empty(),
    };
    let schema_env = ModuleEnv::from_module(&schema_module, &schema_path)?;

    let mut evaluator = Evaluator::new(validators_env, schema_env);
    let schema_tables = evaluator.extract_schema_tables(&schema_module)?;

    let mut named_types: IndexMap<String, TypeExpr> = IndexMap::new();

    // Include all validator consts (excluding pure object-literals used for spreads)
    let validator_items: Vec<(String, Expr)> = evaluator
        .validators_env
        .decls
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    for (name, expr) in validator_items {
        if evaluator.validators_env.object_literals.contains(&name) {
            continue;
        }
        if is_validator_expr(&expr) {
            let ty = evaluator.eval_expr(&expr)?;
            named_types.insert(name.clone(), ty);
        }
    }

    // Include schema consts (excluding object-literals used for spreads)
    let schema_items: Vec<(String, Expr)> = evaluator
        .schema_env
        .decls
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    for (name, expr) in schema_items {
        if evaluator.schema_env.object_literals.contains(&name) {
            continue;
        }
        if is_validator_expr(&expr) {
            let ty = evaluator.eval_expr(&expr)?;
            named_types.insert(name.clone(), ty);
        }
    }

    let mut name_map: HashMap<String, String> = named_types
        .keys()
        .map(|name| (name.clone(), pascal_case(name)))
        .collect();
    let function_args = extract_function_args(&mut evaluator, &config.convex_dir)?;
    for def in &function_args {
        let args_key = format!("args_{}_{}", def.module, def.name);
        let return_key = format!("return_{}_{}", def.module, def.name);
        name_map.insert(args_key, function_args_type_name(&def.module, &def.name));
        name_map.insert(
            return_key,
            function_return_type_name(&def.module, &def.name),
        );
    }
    let mut swift_name_to_key: HashMap<String, String> = name_map
        .iter()
        .map(|(k, v)| (v.clone(), k.clone()))
        .collect();

    let mut table_key_map: HashMap<String, String> = HashMap::new();

    // Include table document types
    for table in schema_tables {
        let swift_name = table_type_name(&table.name);
        let mut doc_type = resolve_named_type(&named_types, &table.doc_type);
        if let TypeExpr::Object(ref mut fields) = doc_type {
            if !fields.contains_key("_id") {
                fields.insert(
                    "_id".to_string(),
                    Field {
                        ty: TypeExpr::String,
                        optional: false,
                        nullable: false,
                    },
                );
            }
        }

        if let Some(existing_key) = swift_name_to_key.get(&swift_name).cloned() {
            named_types.insert(existing_key.clone(), doc_type);
            table_key_map.insert(table.name, existing_key);
        } else {
            let key = format!("table_{}", table.name);
            named_types.insert(key.clone(), doc_type);
            name_map.insert(key.clone(), swift_name.clone());
            swift_name_to_key.insert(swift_name, key.clone());
            table_key_map.insert(table.name, key);
        }
    }

    let function_return_types = extract_return_types(&config.convex_dir, &table_key_map)?;
    let convex_function_defs =
        build_function_defs(function_args, function_return_types, &mut named_types)?;

    let rust_name_map = name_map.clone();
    let swift_function_defs = convex_function_defs
        .iter()
        .filter(|def| config.include_internal_functions || !def.is_internal)
        .map(|def| SwiftFunctionDef {
            module: def.module.clone(),
            name: def.name.clone(),
            kind: def.kind.clone(),
            args_key: def.args_key.clone(),
            args_ty: def.args_ty.clone(),
        })
        .collect::<Vec<_>>();

    let mut generator = SwiftGenerator::new(&named_types, name_map, swift_function_defs);
    let swift = generator.generate()?;

    fs::create_dir_all(&config.swift_out)
        .with_context(|| format!("Failed to create {:?}", config.swift_out))?;
    let support_path = config.swift_out.join("ConvexSupport.swift");
    let types_path = config.swift_out.join("ConvexTypes.swift");
    let api_path = config.swift_out.join("ConvexAPI.swift");

    fs::write(&support_path, swift.support)
        .with_context(|| format!("Failed to write {:?}", support_path))?;
    fs::write(&types_path, swift.types)
        .with_context(|| format!("Failed to write {:?}", types_path))?;
    fs::write(&api_path, swift.api).with_context(|| format!("Failed to write {:?}", api_path))?;

    let rust_args = convex_function_defs
        .iter()
        .map(|def| {
            rust_name_map
                .get(&def.args_key)
                .cloned()
                .ok_or_else(|| anyhow!("Missing Rust name for args key {}", def.args_key))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut rust_generator = RustGenerator::new(
        &named_types,
        rust_name_map.clone(),
        rust_args,
        rust_convex_value_path,
    );
    let rust = rust_generator.generate()?;
    if let Some(parent) = config.rust_out.parent() {
        fs::create_dir_all(parent).with_context(|| format!("Failed to create {:?}", parent))?;
    }
    fs::write(&config.rust_out, rust)
        .with_context(|| format!("Failed to write {:?}", config.rust_out))?;

    let rust_api_defs = convex_function_defs
        .iter()
        .map(|def| {
            let args_name = rust_name_map
                .get(&def.args_key)
                .cloned()
                .ok_or_else(|| anyhow!("Missing Rust name for args key {}", def.args_key))?;
            let return_name = rust_name_map
                .get(&def.return_key)
                .cloned()
                .ok_or_else(|| anyhow!("Missing Rust name for return key {}", def.return_key))?;
            Ok(RustFunctionDef {
                module: def.module.clone(),
                name: def.name.clone(),
                kind: def.kind.clone(),
                args_name,
                return_name,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let rust_api = generate_rust_api(
        &rust_api_defs,
        &rust_api_client_module_path,
        &rust_api_types_module_path,
    )?;
    if let Some(parent) = config.rust_api_out.parent() {
        fs::create_dir_all(parent).with_context(|| format!("Failed to create {:?}", parent))?;
    }
    fs::write(&config.rust_api_out, rust_api)
        .with_context(|| format!("Failed to write {:?}", config.rust_api_out))?;

    Ok(())
}

fn parse_ts_module(path: &Path) -> Result<Module> {
    let source = fs::read_to_string(path).with_context(|| format!("Failed to read {:?}", path))?;
    let cm: SourceMap = Default::default();
    let fm = cm.new_source_file(FileName::Real(path.to_path_buf()).into(), source);
    let syntax = Syntax::Typescript(TsSyntax {
        tsx: false,
        decorators: false,
        ..Default::default()
    });
    let mut parser = Parser::new(syntax, StringInput::from(&*fm), None);

    parser
        .parse_module()
        .map_err(|err| anyhow!("TS parse error in {:?}: {:?}", path, err))
}

fn parse_optional_ts_module(path: &Path) -> Result<Option<Module>> {
    if path.is_file() {
        return Ok(Some(parse_ts_module(path)?));
    }
    Ok(None)
}

#[derive(Clone, Debug, PartialEq)]
enum TypeExpr {
    Named(String),
    String,
    Number,
    Int64,
    Float64,
    Boolean,
    Any,
    Null,
    LiteralString(String),
    LiteralNumber(f64),
    Id(String),
    Object(BTreeMap<String, Field>),
    Array(Box<TypeExpr>),
    Record(Box<TypeExpr>, Box<TypeExpr>),
    Union(Vec<TypeExpr>),
    Optional(Box<TypeExpr>),
    Nullable(Box<TypeExpr>),
}

#[derive(Clone, Debug, PartialEq)]
struct Field {
    ty: TypeExpr,
    optional: bool,
    nullable: bool,
}

#[derive(Clone, Debug)]
struct TableDef {
    name: String,
    doc_type: TypeExpr,
}

#[derive(Clone, Debug, PartialEq)]
enum ConvexFunctionKind {
    Query,
    Mutation,
    Action,
}

#[derive(Clone, Debug)]
struct FunctionArgDef {
    module: String,
    name: String,
    kind: ConvexFunctionKind,
    is_internal: bool,
    ty: TypeExpr,
}

fn resolve_named_type(types: &IndexMap<String, TypeExpr>, ty: &TypeExpr) -> TypeExpr {
    match ty {
        TypeExpr::Named(name) => {
            if let Some(inner) = types.get(name) {
                resolve_named_type(types, inner)
            } else {
                ty.clone()
            }
        }
        _ => ty.clone(),
    }
}

#[derive(Clone, Debug)]
struct ModuleEnv {
    decls: HashMap<String, Expr>,
    object_literals: HashSet<String>,
}

impl ModuleEnv {
    fn empty() -> Self {
        Self {
            decls: HashMap::new(),
            object_literals: HashSet::new(),
        }
    }

    fn merge(&self, other: &Self) -> Self {
        let mut decls = self.decls.clone();
        decls.extend(other.decls.clone());

        let mut object_literals = self.object_literals.clone();
        object_literals.extend(other.object_literals.iter().cloned());

        Self {
            decls,
            object_literals,
        }
    }

    fn from_module(module: &Module, _path: &Path) -> Result<Self> {
        let mut decls = HashMap::new();
        let mut object_literals = HashSet::new();

        for item in &module.body {
            match item {
                ModuleItem::ModuleDecl(ModuleDecl::ExportDecl(ExportDecl {
                    decl: Decl::Var(var_decl),
                    ..
                })) => {
                    for decl in &var_decl.decls {
                        if let Pat::Ident(ident) = &decl.name {
                            if let Some(init) = &decl.init {
                                decls.insert(ident.sym.to_string(), *init.clone());
                                if matches!(&**init, Expr::Object(_)) {
                                    object_literals.insert(ident.sym.to_string());
                                }
                            }
                        }
                    }
                }
                ModuleItem::Stmt(Stmt::Decl(Decl::Var(var_decl))) => {
                    for decl in &var_decl.decls {
                        if let Pat::Ident(ident) = &decl.name {
                            if let Some(init) = &decl.init {
                                decls.insert(ident.sym.to_string(), *init.clone());
                                if matches!(&**init, Expr::Object(_)) {
                                    object_literals.insert(ident.sym.to_string());
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(Self {
            decls,
            object_literals,
        })
    }
}

struct Evaluator {
    validators_env: ModuleEnv,
    schema_env: ModuleEnv,
    cache: HashMap<String, TypeExpr>,
    object_cache: HashMap<String, BTreeMap<String, Field>>,
}

impl Evaluator {
    fn new(validators_env: ModuleEnv, schema_env: ModuleEnv) -> Self {
        Self {
            validators_env,
            schema_env,
            cache: HashMap::new(),
            object_cache: HashMap::new(),
        }
    }

    fn extract_schema_tables(&mut self, schema_module: &Module) -> Result<Vec<TableDef>> {
        for item in &schema_module.body {
            if let ModuleItem::ModuleDecl(ModuleDecl::ExportDefaultExpr(expr)) = item {
                return self.parse_define_schema(&expr.expr);
            }
        }
        Err(anyhow!(
            "schema.ts: export default defineSchema(...) not found"
        ))
    }

    fn parse_define_schema(&mut self, expr: &Expr) -> Result<Vec<TableDef>> {
        let call = match expr {
            Expr::Call(call) => call,
            _ => return Err(anyhow!("schema.ts: default export is not a call")),
        };

        let callee_name = match &call.callee {
            Callee::Expr(callee_expr) => match &**callee_expr {
                Expr::Ident(ident) => ident.sym.to_string(),
                _ => String::new(),
            },
            _ => String::new(),
        };

        if callee_name != "defineSchema" {
            return Err(anyhow!("schema.ts: default export is not defineSchema"));
        }

        let schema_arg = call
            .args
            .first()
            .ok_or_else(|| anyhow!("schema.ts: defineSchema missing argument"))?;

        let schema_obj = match &*schema_arg.expr {
            Expr::Object(obj) => obj,
            _ => return Err(anyhow!("schema.ts: defineSchema arg is not object literal")),
        };

        let mut tables = Vec::new();

        for prop in &schema_obj.props {
            let (key, value_expr) = match prop {
                PropOrSpread::Prop(prop) => match &**prop {
                    Prop::KeyValue(kv) => (prop_name(&kv.key)?, &*kv.value),
                    _ => continue,
                },
                _ => continue,
            };

            let table_arg = extract_define_table_arg(value_expr)
                .ok_or_else(|| anyhow!("schema.ts: {} is not defineTable(...)", key))?;
            let doc_type = self.eval_expr(table_arg)?;
            tables.push(TableDef {
                name: key,
                doc_type,
            });
        }

        Ok(tables)
    }

    fn eval_expr(&mut self, expr: &Expr) -> Result<TypeExpr> {
        match expr {
            Expr::Call(call) => self.eval_call(call),
            Expr::Ident(ident) => self.eval_ident(ident.sym.as_ref()),
            Expr::Object(obj) => {
                let fields = self.eval_object_fields(obj)?;
                Ok(TypeExpr::Object(fields))
            }
            _ => Err(anyhow!("Unsupported expr: {:?}", expr)),
        }
    }

    fn eval_ident(&mut self, name: &str) -> Result<TypeExpr> {
        if let Some(cached) = self.cache.get(name) {
            return Ok(cached.clone());
        }
        let exists = self.validators_env.decls.contains_key(name)
            || self.schema_env.decls.contains_key(name);
        if !exists {
            return Err(anyhow!("Unknown identifier: {}", name));
        }
        let ty = TypeExpr::Named(name.to_string());
        self.cache.insert(name.to_string(), ty.clone());
        Ok(ty)
    }

    fn eval_call(&mut self, call: &CallExpr) -> Result<TypeExpr> {
        let (object, method) = match &call.callee {
            Callee::Expr(expr) => match &**expr {
                Expr::Member(member) => (member.obj.clone(), member.prop.clone()),
                _ => return Err(anyhow!("Unsupported call callee")),
            },
            _ => return Err(anyhow!("Unsupported call callee")),
        };

        let obj_ident = match &*object {
            Expr::Ident(ident) => ident.sym.to_string(),
            _ => return Err(anyhow!("Unsupported call target")),
        };

        if obj_ident != "v" {
            return Err(anyhow!("Unsupported call target: {}", obj_ident));
        }

        let method_name = match member_prop_name(&method) {
            Ok(name) => name,
            Err(_) => return Err(anyhow!("Unsupported call method")),
        };

        match method_name.as_str() {
            "string" => Ok(TypeExpr::String),
            "number" => Ok(TypeExpr::Number),
            "int64" => Ok(TypeExpr::Int64),
            "float64" => Ok(TypeExpr::Float64),
            "boolean" => Ok(TypeExpr::Boolean),
            "any" => Ok(TypeExpr::Any),
            "null" => Ok(TypeExpr::Null),
            "literal" => {
                let arg = call
                    .args
                    .first()
                    .ok_or_else(|| anyhow!("v.literal missing arg"))?;
                match &*arg.expr {
                    Expr::Lit(Lit::Str(s)) => Ok(TypeExpr::LiteralString(
                        s.value.to_string_lossy().into_owned(),
                    )),
                    Expr::Lit(Lit::Num(n)) => Ok(TypeExpr::LiteralNumber(n.value)),
                    _ => Err(anyhow!("v.literal only supports string/number")),
                }
            }
            "id" => {
                let arg = call
                    .args
                    .first()
                    .ok_or_else(|| anyhow!("v.id missing arg"))?;
                match &*arg.expr {
                    Expr::Lit(Lit::Str(s)) => {
                        Ok(TypeExpr::Id(s.value.to_string_lossy().into_owned()))
                    }
                    _ => Err(anyhow!("v.id requires string literal")),
                }
            }
            "object" => {
                let arg = call
                    .args
                    .first()
                    .ok_or_else(|| anyhow!("v.object missing arg"))?;
                let obj = match &*arg.expr {
                    Expr::Object(obj) => obj,
                    _ => return Err(anyhow!("v.object requires object literal")),
                };
                let fields = self.eval_object_fields(obj)?;
                Ok(TypeExpr::Object(fields))
            }
            "array" => {
                let arg = call
                    .args
                    .first()
                    .ok_or_else(|| anyhow!("v.array missing arg"))?;
                let inner = self.eval_expr(&arg.expr)?;
                Ok(TypeExpr::Array(Box::new(inner)))
            }
            "record" => {
                let key_arg = call
                    .args
                    .first()
                    .ok_or_else(|| anyhow!("v.record missing key"))?;
                let val_arg = call
                    .args
                    .get(1)
                    .ok_or_else(|| anyhow!("v.record missing val"))?;
                let key_ty = self.eval_expr(&key_arg.expr)?;
                let val_ty = self.eval_expr(&val_arg.expr)?;
                Ok(TypeExpr::Record(Box::new(key_ty), Box::new(val_ty)))
            }
            "union" => {
                if call.args.is_empty() {
                    return Err(anyhow!("v.union requires args"));
                }
                let mut members = Vec::new();
                for arg in &call.args {
                    members.push(self.eval_expr(&arg.expr)?);
                }
                if members
                    .iter()
                    .any(|member| matches!(member, TypeExpr::Null))
                {
                    let filtered = members
                        .into_iter()
                        .filter(|member| !matches!(member, TypeExpr::Null))
                        .collect::<Vec<_>>();
                    if filtered.is_empty() {
                        return Ok(TypeExpr::Null);
                    }
                    if filtered.len() == 1 {
                        return Ok(TypeExpr::Nullable(Box::new(filtered[0].clone())));
                    }
                    return Ok(TypeExpr::Nullable(Box::new(TypeExpr::Union(filtered))));
                }
                Ok(TypeExpr::Union(members))
            }
            "optional" => {
                let arg = call
                    .args
                    .first()
                    .ok_or_else(|| anyhow!("v.optional missing arg"))?;
                let inner = self.eval_expr(&arg.expr)?;
                Ok(TypeExpr::Optional(Box::new(inner)))
            }
            _ => Err(anyhow!("Unsupported v.{} call", method_name)),
        }
    }

    fn eval_object_fields(&mut self, obj: &ObjectLit) -> Result<BTreeMap<String, Field>> {
        let mut fields = BTreeMap::new();

        for prop in &obj.props {
            match prop {
                PropOrSpread::Spread(spread) => {
                    let ident = match &*spread.expr {
                        Expr::Ident(ident) => ident.sym.to_string(),
                        _ => return Err(anyhow!("Unsupported spread expression")),
                    };
                    let spread_fields = self.eval_object_literal_ident(&ident)?;
                    for (k, v) in spread_fields {
                        fields.insert(k, v);
                    }
                }
                PropOrSpread::Prop(prop) => match &**prop {
                    Prop::KeyValue(kv) => {
                        let key = prop_name(&kv.key)?;
                        let ty = self.eval_expr(&kv.value)?;
                        fields.insert(key, normalize_field_type(ty));
                    }
                    Prop::Shorthand(ident) => {
                        let key = ident.sym.to_string();
                        let ty = self.eval_ident(&key)?;
                        fields.insert(key, normalize_field_type(ty));
                    }
                    _ => return Err(anyhow!("Unsupported object property")),
                },
            }
        }

        Ok(fields)
    }

    fn eval_object_literal_ident(&mut self, ident: &str) -> Result<BTreeMap<String, Field>> {
        if let Some(cached) = self.object_cache.get(ident) {
            return Ok(cached.clone());
        }
        let expr = self
            .validators_env
            .decls
            .get(ident)
            .or_else(|| self.schema_env.decls.get(ident))
            .cloned()
            .ok_or_else(|| anyhow!("Unknown spread identifier: {}", ident))?;

        let obj = match expr {
            Expr::Object(obj) => obj,
            _ => {
                return Err(anyhow!(
                    "Spread identifier is not object literal: {}",
                    ident
                ));
            }
        };

        let fields = self.eval_object_fields(&obj)?;
        self.object_cache.insert(ident.to_string(), fields.clone());
        Ok(fields)
    }

    fn extract_function_args_from_module(
        &self,
        module: &Module,
    ) -> Result<Vec<(String, ConvexFunctionKind, bool, TypeExpr)>> {
        let module_env = ModuleEnv::from_module(module, Path::new("<module>"))?;
        let combined_validators_env = self.validators_env.merge(&module_env);
        let mut module_evaluator = Evaluator::new(combined_validators_env, self.schema_env.clone());
        let mut out = Vec::new();

        for item in &module.body {
            let ModuleItem::ModuleDecl(ModuleDecl::ExportDecl(ExportDecl { decl, .. })) = item
            else {
                continue;
            };
            let Decl::Var(var_decl) = decl else {
                continue;
            };
            for decl in &var_decl.decls {
                let Pat::Ident(ident) = &decl.name else {
                    continue;
                };
                let Some(init) = &decl.init else {
                    continue;
                };
                let Expr::Call(call) = &**init else {
                    continue;
                };
                let Some(kind_info) = function_kind(call) else {
                    continue;
                };
                let args_expr = extract_args_expr(call)?;
                let ty = match args_expr {
                    Some(expr) => module_evaluator.eval_expr(expr)?,
                    None => TypeExpr::Object(BTreeMap::new()),
                };
                let ty = inline_module_named_types(ty, &module_env, &mut module_evaluator)?;
                out.push((
                    ident.sym.to_string(),
                    kind_info.kind,
                    kind_info.is_internal,
                    ty,
                ));
            }
        }

        Ok(out)
    }
}

fn normalize_field_type(mut ty: TypeExpr) -> Field {
    let mut optional = false;
    let mut nullable = false;

    loop {
        match ty {
            TypeExpr::Optional(inner) => {
                ty = *inner;
                optional = true;
            }
            TypeExpr::Nullable(inner) => {
                ty = *inner;
                nullable = true;
            }
            _ => break,
        }
    }

    Field {
        ty,
        optional,
        nullable,
    }
}

fn inline_module_named_types(
    ty: TypeExpr,
    module_env: &ModuleEnv,
    evaluator: &mut Evaluator,
) -> Result<TypeExpr> {
    let mut visiting = HashSet::new();
    inline_module_named_types_inner(ty, module_env, evaluator, &mut visiting)
}

fn inline_module_named_types_inner(
    ty: TypeExpr,
    module_env: &ModuleEnv,
    evaluator: &mut Evaluator,
    visiting: &mut HashSet<String>,
) -> Result<TypeExpr> {
    match ty {
        TypeExpr::Named(name) => {
            if !module_env.decls.contains_key(&name) {
                return Ok(TypeExpr::Named(name));
            }
            if !visiting.insert(name.clone()) {
                return Err(anyhow!("recursive local validator identifier: {}", name));
            }

            let expr = module_env
                .decls
                .get(&name)
                .ok_or_else(|| anyhow!("unknown local validator identifier: {}", name))?;
            let resolved = evaluator.eval_expr(expr)?;
            let inlined =
                inline_module_named_types_inner(resolved, module_env, evaluator, visiting)?;
            visiting.remove(&name);
            Ok(inlined)
        }
        TypeExpr::Object(fields) => {
            let mut out = BTreeMap::new();
            for (name, field) in fields {
                let inlined =
                    inline_module_named_types_inner(field.ty, module_env, evaluator, visiting)?;
                out.insert(
                    name,
                    Field {
                        ty: inlined,
                        optional: field.optional,
                        nullable: field.nullable,
                    },
                );
            }
            Ok(TypeExpr::Object(out))
        }
        TypeExpr::Array(inner) => Ok(TypeExpr::Array(Box::new(inline_module_named_types_inner(
            *inner, module_env, evaluator, visiting,
        )?))),
        TypeExpr::Record(key, value) => Ok(TypeExpr::Record(
            Box::new(inline_module_named_types_inner(
                *key, module_env, evaluator, visiting,
            )?),
            Box::new(inline_module_named_types_inner(
                *value, module_env, evaluator, visiting,
            )?),
        )),
        TypeExpr::Union(members) => {
            let mut out = Vec::new();
            for member in members {
                out.push(inline_module_named_types_inner(
                    member, module_env, evaluator, visiting,
                )?);
            }
            Ok(TypeExpr::Union(out))
        }
        TypeExpr::Optional(inner) => Ok(TypeExpr::Optional(Box::new(
            inline_module_named_types_inner(*inner, module_env, evaluator, visiting)?,
        ))),
        TypeExpr::Nullable(inner) => Ok(TypeExpr::Nullable(Box::new(
            inline_module_named_types_inner(*inner, module_env, evaluator, visiting)?,
        ))),
        other => Ok(other),
    }
}

fn is_validator_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Call(_) | Expr::Object(_) | Expr::Ident(_))
}

fn extract_function_args(
    evaluator: &mut Evaluator,
    convex_dir: &Path,
) -> Result<Vec<FunctionArgDef>> {
    let mut files = Vec::new();
    collect_typescript_files(convex_dir, &mut files)?;
    files.sort();

    let mut out = Vec::new();
    for path in files {
        let module_name = module_name_from_path(convex_dir, &path)?;
        if matches!(
            module_name.as_str(),
            "schema" | "validators" | "auth.config"
        ) {
            continue;
        }
        let module = parse_ts_module(&path)?;
        let args = evaluator.extract_function_args_from_module(&module)?;
        for (name, kind, is_internal, ty) in args {
            out.push(FunctionArgDef {
                module: module_name.clone(),
                name,
                kind,
                is_internal,
                ty,
            });
        }
    }
    Ok(out)
}

fn collect_typescript_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().and_then(|name| name.to_str()) == Some("_generated") {
                continue;
            }
            collect_typescript_files(&path, out)?;
            continue;
        }

        if path.extension().and_then(|ext| ext.to_str()) != Some("ts") {
            continue;
        }
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".d.ts"))
        {
            continue;
        }
        out.push(path);
    }
    Ok(())
}

fn module_name_from_path(convex_dir: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(convex_dir)
        .map_err(|_| anyhow!("Failed to strip convex dir prefix from {:?}", path))?;
    let relative = relative.to_string_lossy().replace('\\', "/");
    relative
        .strip_suffix(".ts")
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("Invalid TypeScript module path: {:?}", path))
}

#[derive(Clone, Debug)]
struct ConvexFunctionDef {
    module: String,
    name: String,
    kind: ConvexFunctionKind,
    is_internal: bool,
    args_key: String,
    args_ty: TypeExpr,
    return_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ReturnTypeRepr {
    String,
    Number,
    Boolean,
    Int64,
    Any,
    Null,
    Undefined,
    LiteralString {
        value: String,
    },
    LiteralNumber {
        value: f64,
    },
    Array {
        items: Box<ReturnTypeRepr>,
    },
    Record {
        key: Box<ReturnTypeRepr>,
        value: Box<ReturnTypeRepr>,
    },
    Union {
        members: Vec<ReturnTypeRepr>,
    },
    Object {
        fields: BTreeMap<String, ReturnFieldRepr>,
    },
    Doc {
        table: String,
    },
    Id {
        table: String,
    },
}

#[derive(Debug, Deserialize)]
struct ReturnFieldRepr {
    #[serde(rename = "type")]
    ty: ReturnTypeRepr,
    optional: bool,
}

fn extract_function_return_types(
    convex_dir: &Path,
    table_key_map: &HashMap<String, String>,
) -> Result<HashMap<String, TypeExpr>> {
    let output = Command::new("node")
        .arg("--input-type=module")
        .arg("--eval")
        .arg(include_str!("extract_convex_function_return_types.mjs"))
        .arg("--")
        .arg("--convex-dir")
        .arg(convex_dir)
        .output()
        .with_context(|| {
            format!(
                "failed to run node for Convex return type extraction. Make sure Node.js 20+ is installed and available on PATH (convex dir: {:?})",
                convex_dir
            )
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!(
            "convex return type extraction failed: {}",
            stderr.trim()
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let reprs: HashMap<String, ReturnTypeRepr> = serde_json::from_str(stdout.trim())
        .map_err(|err| anyhow!("invalid return type extractor JSON: {}", err))?;

    let mut out = HashMap::new();
    for (key, repr) in reprs {
        out.insert(key, return_type_repr_to_type_expr(&repr, table_key_map)?);
    }
    Ok(out)
}

fn return_type_repr_to_type_expr(
    repr: &ReturnTypeRepr,
    table_key_map: &HashMap<String, String>,
) -> Result<TypeExpr> {
    Ok(match repr {
        ReturnTypeRepr::String => TypeExpr::String,
        ReturnTypeRepr::Number => TypeExpr::Number,
        ReturnTypeRepr::Boolean => TypeExpr::Boolean,
        ReturnTypeRepr::Int64 => TypeExpr::Int64,
        ReturnTypeRepr::Any => TypeExpr::Any,
        ReturnTypeRepr::Null => TypeExpr::Null,
        ReturnTypeRepr::Undefined => TypeExpr::Null,
        ReturnTypeRepr::LiteralString { value } => TypeExpr::LiteralString(value.clone()),
        ReturnTypeRepr::LiteralNumber { value } => TypeExpr::LiteralNumber(*value),
        ReturnTypeRepr::Array { items } => TypeExpr::Array(Box::new(
            return_type_repr_to_type_expr(items, table_key_map)?,
        )),
        ReturnTypeRepr::Record { key, value } => TypeExpr::Record(
            Box::new(return_type_repr_to_type_expr(key, table_key_map)?),
            Box::new(return_type_repr_to_type_expr(value, table_key_map)?),
        ),
        ReturnTypeRepr::Union { members } => {
            let mut converted = members
                .iter()
                .map(|member| return_type_repr_to_type_expr(member, table_key_map))
                .collect::<Result<Vec<_>>>()?;
            normalize_nullable_union(&mut converted)
        }
        ReturnTypeRepr::Object { fields } => {
            let mut out_fields = BTreeMap::new();
            for (name, field) in fields {
                let mut ty = return_type_repr_to_type_expr(&field.ty, table_key_map)?;
                let mut nullable = false;
                if let TypeExpr::Nullable(inner) = ty {
                    nullable = true;
                    ty = *inner;
                }
                out_fields.insert(
                    name.clone(),
                    Field {
                        ty,
                        optional: field.optional,
                        nullable,
                    },
                );
            }
            TypeExpr::Object(out_fields)
        }
        ReturnTypeRepr::Doc { table } => {
            let key = table_key_map
                .get(table)
                .cloned()
                .ok_or_else(|| anyhow!("unknown table in return type: {}", table))?;
            TypeExpr::Named(key)
        }
        ReturnTypeRepr::Id { table } => TypeExpr::Id(table.clone()),
    })
}

fn normalize_nullable_union(members: &mut Vec<TypeExpr>) -> TypeExpr {
    let mut flattened = Vec::new();
    for member in members.drain(..) {
        match member {
            TypeExpr::Union(inner) => flattened.extend(inner),
            other => flattened.push(other),
        }
    }

    let mut non_null = Vec::new();
    let mut has_null = false;
    for member in flattened {
        if member == TypeExpr::Null {
            has_null = true;
            continue;
        }
        if !non_null.contains(&member) {
            non_null.push(member);
        }
    }

    if non_null.is_empty() {
        return TypeExpr::Null;
    }

    let inner = if non_null.len() == 1 {
        non_null.into_iter().next().unwrap()
    } else {
        TypeExpr::Union(non_null)
    };

    if has_null {
        TypeExpr::Nullable(Box::new(inner))
    } else {
        inner
    }
}

fn build_function_defs(
    function_args: Vec<FunctionArgDef>,
    function_return_types: HashMap<String, TypeExpr>,
    named_types: &mut IndexMap<String, TypeExpr>,
) -> Result<Vec<ConvexFunctionDef>> {
    let mut out = Vec::new();
    for arg_def in function_args {
        let args_key = format!("args_{}_{}", arg_def.module, arg_def.name);
        named_types.insert(args_key.clone(), arg_def.ty.clone());

        let return_key = format!("return_{}_{}", arg_def.module, arg_def.name);
        let return_lookup_key = format!("{}:{}", arg_def.module, arg_def.name);
        let return_ty = function_return_types
            .get(&return_lookup_key)
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "missing return type for convex function {}",
                    return_lookup_key
                )
            })?;
        named_types.insert(return_key.clone(), return_ty);

        out.push(ConvexFunctionDef {
            module: arg_def.module,
            name: arg_def.name,
            kind: arg_def.kind,
            is_internal: arg_def.is_internal,
            args_key,
            args_ty: arg_def.ty,
            return_key,
        });
    }
    Ok(out)
}

struct FunctionKindInfo {
    kind: ConvexFunctionKind,
    is_internal: bool,
}

fn function_kind(call: &CallExpr) -> Option<FunctionKindInfo> {
    let Callee::Expr(expr) = &call.callee else {
        return None;
    };
    let Expr::Ident(ident) = &**expr else {
        return None;
    };
    match ident.sym.as_ref() {
        "query" => Some(FunctionKindInfo {
            kind: ConvexFunctionKind::Query,
            is_internal: false,
        }),
        "internalQuery" => Some(FunctionKindInfo {
            kind: ConvexFunctionKind::Query,
            is_internal: true,
        }),
        "mutation" => Some(FunctionKindInfo {
            kind: ConvexFunctionKind::Mutation,
            is_internal: false,
        }),
        "internalMutation" => Some(FunctionKindInfo {
            kind: ConvexFunctionKind::Mutation,
            is_internal: true,
        }),
        "action" => Some(FunctionKindInfo {
            kind: ConvexFunctionKind::Action,
            is_internal: false,
        }),
        "internalAction" => Some(FunctionKindInfo {
            kind: ConvexFunctionKind::Action,
            is_internal: true,
        }),
        _ => None,
    }
}

fn extract_args_expr(call: &CallExpr) -> Result<Option<&Expr>> {
    let Some(arg) = call.args.first() else {
        return Ok(None);
    };
    let obj = match &*arg.expr {
        Expr::Object(obj) => obj,
        _ => return Err(anyhow!("Convex function call missing args object literal")),
    };
    for prop in &obj.props {
        let PropOrSpread::Prop(prop) = prop else {
            continue;
        };
        let Prop::KeyValue(kv) = &**prop else {
            continue;
        };
        if prop_name(&kv.key)? == "args" {
            return Ok(Some(&*kv.value));
        }
    }
    Ok(None)
}

fn extract_define_table_arg(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::Call(call) => match &call.callee {
            Callee::Expr(callee_expr) => match &**callee_expr {
                Expr::Ident(ident) if ident.sym == *"defineTable" => {
                    call.args.first().map(|arg| &*arg.expr)
                }
                Expr::Member(member) => extract_define_table_arg(&member.obj),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

fn prop_name(prop: &PropName) -> Result<String> {
    match prop {
        PropName::Ident(ident) => Ok(ident.sym.to_string()),
        PropName::Str(s) => Ok(s.value.to_string_lossy().into_owned()),
        PropName::Num(n) => Ok(n.value.to_string()),
        _ => Err(anyhow!("Unsupported prop name")),
    }
}

fn member_prop_name(prop: &MemberProp) -> Result<String> {
    match prop {
        MemberProp::Ident(ident) => Ok(ident.sym.to_string()),
        MemberProp::Computed(computed) => match &*computed.expr {
            Expr::Lit(Lit::Str(s)) => Ok(s.value.to_string_lossy().into_owned()),
            _ => Err(anyhow!("Unsupported computed member prop")),
        },
        _ => Err(anyhow!("Unsupported member prop")),
    }
}

struct SwiftOutput {
    support: String,
    types: String,
    api: String,
}

#[derive(Clone, Debug)]
struct SwiftFunctionDef {
    module: String,
    name: String,
    kind: ConvexFunctionKind,
    args_key: String,
    args_ty: TypeExpr,
}

#[derive(Clone, Debug)]
struct VariantInfo {
    literal: String,
    fields: BTreeMap<String, Field>,
    type_name: Option<String>,
}

#[derive(Clone, Debug)]
struct LiteralEnumInfo {
    enum_def: String,
    literals: Vec<String>,
}

struct SwiftGenerator {
    used_names: HashSet<String>,
    defs: Vec<String>,
    args_impls: Vec<String>,
    name_map: HashMap<String, String>,
    types: HashMap<String, TypeExpr>,
    function_defs: Vec<SwiftFunctionDef>,
}

#[derive(Clone, Debug)]
struct SwiftArgField {
    swift_name: String,
    swift_type: String,
    is_optional: bool,
}

#[derive(Clone, Copy, Debug)]
enum InlineWrapperKind {
    Query,
    Mutation,
    Action,
}

impl SwiftGenerator {
    fn new(
        types: &IndexMap<String, TypeExpr>,
        name_map: HashMap<String, String>,
        function_defs: Vec<SwiftFunctionDef>,
    ) -> Self {
        let types_map = types.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        Self {
            used_names: HashSet::new(),
            defs: Vec::new(),
            args_impls: Vec::new(),
            name_map,
            types: types_map,
            function_defs,
        }
    }

    fn generate(&mut self) -> Result<SwiftOutput> {
        let mut names: Vec<_> = self.types.keys().cloned().collect();
        names.sort();

        for name in names {
            let expr = self.types.get(&name).cloned().unwrap();
            let swift_name = self.name_map.get(&name).unwrap().clone();
            self.emit_named_type(&swift_name, &expr)?;
        }

        let support = self.emit_support();
        let api = self.emit_api()?;
        let mut types_out = String::new();
        types_out.push_str("// swift-format-ignore-file\n");
        types_out.push_str("// This file is @generated by hull. DO NOT EDIT.\n");
        types_out.push_str("import Foundation\n");
        types_out.push_str("@preconcurrency import ConvexMobile\n\n");
        for def in &self.defs {
            types_out.push_str(def);
            types_out.push_str("\n\n");
        }
        for def in &self.args_impls {
            types_out.push_str(def);
            types_out.push_str("\n\n");
        }

        Ok(SwiftOutput {
            support,
            types: types_out,
            api,
        })
    }

    fn emit_support(&self) -> String {
        let mut out = String::new();
        out.push_str("// swift-format-ignore-file\n");
        out.push_str("// This file is @generated by hull. DO NOT EDIT.\n");
        out.push_str("import Foundation\n");
        out.push_str("@preconcurrency import ConvexMobile\n\n");
        out.push_str("protocol ConvexCodable: Codable, ConvexEncodable {}\n\n");
        out.push_str("enum JSONValue: Codable, Equatable, Sendable {\n");
        out.push_str("    case object([String: JSONValue])\n");
        out.push_str("    case array([JSONValue])\n");
        out.push_str("    case string(String)\n");
        out.push_str("    case number(Double)\n");
        out.push_str("    case bool(Bool)\n");
        out.push_str("    case null\n\n");
        out.push_str("    init(from decoder: Decoder) throws {\n");
        out.push_str("        let container = try decoder.singleValueContainer()\n");
        out.push_str("        if container.decodeNil() { self = .null; return }\n");
        out.push_str("        if let value = try? container.decode(Bool.self) { self = .bool(value); return }\n");
        out.push_str("        if let value = try? container.decode(Double.self) { self = .number(value); return }\n");
        out.push_str("        if let value = try? container.decode(String.self) { self = .string(value); return }\n");
        out.push_str("        if let value = try? container.decode([String: JSONValue].self) { self = .object(value); return }\n");
        out.push_str("        if let value = try? container.decode([JSONValue].self) { self = .array(value); return }\n");
        out.push_str("        throw DecodingError.dataCorruptedError(in: container, debugDescription: \"Invalid JSON value\")\n");
        out.push_str("    }\n\n");
        out.push_str("    func encode(to encoder: Encoder) throws {\n");
        out.push_str("        switch self {\n");
        out.push_str("        case .object(let values):\n");
        out.push_str("            try values.encode(to: encoder)\n");
        out.push_str("        case .array(let values):\n");
        out.push_str("            try values.encode(to: encoder)\n");
        out.push_str("        case .string(let value):\n");
        out.push_str("            var container = encoder.singleValueContainer()\n");
        out.push_str("            try container.encode(value)\n");
        out.push_str("        case .number(let value):\n");
        out.push_str("            var container = encoder.singleValueContainer()\n");
        out.push_str("            try container.encode(value)\n");
        out.push_str("        case .bool(let value):\n");
        out.push_str("            var container = encoder.singleValueContainer()\n");
        out.push_str("            try container.encode(value)\n");
        out.push_str("        case .null:\n");
        out.push_str("            var container = encoder.singleValueContainer()\n");
        out.push_str("            try container.encodeNil()\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");
        out.push_str("extension JSONValue: ConvexEncodable {}\n\n");
        out
    }

    fn emit_named_type(&mut self, name: &str, expr: &TypeExpr) -> Result<()> {
        if self.used_names.contains(name) {
            return Ok(());
        }
        self.used_names.insert(name.to_string());

        match expr {
            TypeExpr::Named(target) => {
                let target_name = self.swift_name(target)?;
                self.defs
                    .push(format!("typealias {} = {}", name, target_name));
            }
            TypeExpr::Object(fields) => self.emit_struct(name, fields)?,
            TypeExpr::Union(members) => self.emit_union(name, members)?,
            TypeExpr::Array(inner) => {
                let inner_ty = self.swift_type(inner, name, None)?;
                self.defs
                    .push(format!("typealias {} = [{}]", name, inner_ty));
            }
            TypeExpr::Record(key, value) => {
                let key_ty = self.swift_type(key, name, Some("Key"))?;
                if key_ty != "String" {
                    return Err(anyhow!("Swift only supports String keys for record"));
                }
                let value_ty = self.swift_type(value, name, Some("Value"))?;
                self.defs
                    .push(format!("typealias {} = [String: {}]", name, value_ty));
            }
            TypeExpr::Optional(inner) => {
                let inner_ty = self.swift_type(inner, name, None)?;
                self.defs
                    .push(format!("typealias {} = {}?", name, inner_ty));
            }
            TypeExpr::Nullable(inner) => {
                let inner_ty = self.swift_type(inner, name, None)?;
                self.defs
                    .push(format!("typealias {} = {}?", name, inner_ty));
            }
            TypeExpr::LiteralString(_) | TypeExpr::LiteralNumber(_) => {
                return Err(anyhow!("Top-level literal type not supported: {}", name));
            }
            TypeExpr::String => self.defs.push(format!("typealias {} = String", name)),
            TypeExpr::Number => self.defs.push(format!("typealias {} = Double", name)),
            TypeExpr::Int64 => self.defs.push(format!("typealias {} = Int64", name)),
            TypeExpr::Float64 => self.defs.push(format!("typealias {} = Double", name)),
            TypeExpr::Boolean => self.defs.push(format!("typealias {} = Bool", name)),
            TypeExpr::Any => self.defs.push(format!("typealias {} = JSONValue", name)),
            TypeExpr::Null => self.defs.push(format!("typealias {} = JSONValue", name)),
            TypeExpr::Id(_) => self.defs.push(format!("typealias {} = String", name)),
        }

        Ok(())
    }

    fn emit_struct(&mut self, name: &str, fields: &BTreeMap<String, Field>) -> Result<()> {
        let mut out = String::new();
        let conforms = if fields.contains_key("_id") || fields.contains_key("id") {
            "ConvexCodable, Identifiable, Sendable"
        } else {
            "ConvexCodable, Sendable"
        };
        out.push_str(&format!("struct {}: {} {{\n", name, conforms));

        let mut all_keys: Vec<(String, Option<String>, bool, bool)> = Vec::new();
        let mut has_renames = false;
        let mut has_wrappers = false;
        for (field_name, field) in fields {
            let (swift_name, rename) = swift_member_name(field_name);
            let mut ty = self.swift_type(&field.ty, name, Some(field_name))?;
            let (is_optional, is_nullable, inner_resolved) = self.field_optional_nullable(field);
            let inner_resolved = self.resolve_named_expr(&inner_resolved);
            let wrapper = if matches!(inner_resolved, TypeExpr::Int64) {
                if is_optional {
                    Some("OptionalConvexInt")
                } else {
                    Some("ConvexInt")
                }
            } else {
                None
            };
            if wrapper.is_some() {
                has_wrappers = true;
            }
            if is_optional || is_nullable {
                if !ty.ends_with('?') {
                    ty.push('?');
                }
                if let Some(wrapper) = wrapper {
                    out.push_str(&format!("    @{} var {}: {}\n", wrapper, swift_name, ty));
                } else {
                    out.push_str(&format!("    let {}: {}\n", swift_name, ty));
                }
            } else if let Some(wrapper) = wrapper {
                out.push_str(&format!("    @{} var {}: {}\n", wrapper, swift_name, ty));
            } else {
                out.push_str(&format!("    let {}: {}\n", swift_name, ty));
            }
            if rename.is_some() {
                has_renames = true;
            }
            all_keys.push((swift_name, rename, is_optional, is_nullable));
        }

        let needs_encode = has_wrappers;
        if has_renames || needs_encode {
            out.push_str("\n    private enum CodingKeys: String, CodingKey {\n");
            for (swift_name, original, _, _) in &all_keys {
                if let Some(original) = original {
                    out.push_str(&format!("        case {} = \"{}\"\n", swift_name, original));
                } else {
                    out.push_str(&format!("        case {}\n", swift_name));
                }
            }
            out.push_str("    }\n");
        }

        if needs_encode {
            out.push_str("\n    func encode(to encoder: Encoder) throws {\n");
            out.push_str("        var container = encoder.container(keyedBy: CodingKeys.self)\n");
            for (swift_name, _, is_optional, is_nullable) in &all_keys {
                if *is_nullable {
                    out.push_str(&format!("        if let value = {} {{\n", swift_name));
                    out.push_str(&format!(
                        "            try container.encode(value, forKey: .{})\n",
                        swift_name
                    ));
                    out.push_str("        } else {\n");
                    out.push_str(&format!(
                        "            try container.encodeNil(forKey: .{})\n",
                        swift_name
                    ));
                    out.push_str("        }\n");
                } else if *is_optional {
                    out.push_str(&format!(
                        "        try container.encodeIfPresent({}, forKey: .{})\n",
                        swift_name, swift_name
                    ));
                } else {
                    out.push_str(&format!(
                        "        try container.encode({}, forKey: .{})\n",
                        swift_name, swift_name
                    ));
                }
            }
            out.push_str("    }\n");
        }

        if fields.contains_key("_id") {
            out.push_str("\n    var id: String { _id }\n");
        }

        out.push_str("}\n");
        self.defs.push(out);
        Ok(())
    }

    fn emit_union(&mut self, name: &str, members: &[TypeExpr]) -> Result<()> {
        if let Some(enum_def) = self.try_emit_literal_enum(name, members) {
            self.defs.push(enum_def);
            return Ok(());
        }

        if let Some((disc_key, variants)) = self.discriminated_union_info(members) {
            return self.emit_discriminated_union(name, &disc_key, &variants);
        }

        self.emit_untagged_union(name, members)
    }

    fn union_variant_label(&self, parent: &str, member: &TypeExpr, index: usize) -> String {
        let resolved = self.resolve_named_expr(member);
        match resolved {
            TypeExpr::Named(name) => pascal_case(name),
            TypeExpr::String => "String".to_string(),
            TypeExpr::Number => "Number".to_string(),
            TypeExpr::Int64 => "Int64".to_string(),
            TypeExpr::Float64 => "Float64".to_string(),
            TypeExpr::Boolean => "Bool".to_string(),
            TypeExpr::Any => "Any".to_string(),
            TypeExpr::Null => "Null".to_string(),
            TypeExpr::LiteralString(_) | TypeExpr::LiteralNumber(_) => "Literal".to_string(),
            TypeExpr::Id(_) => "Id".to_string(),
            TypeExpr::Array(inner) => {
                let inner_resolved = self.resolve_named_expr(inner);
                if let TypeExpr::Union(members) = inner_resolved {
                    if self.discriminated_union_info(members).is_some() {
                        return "Parts".to_string();
                    }
                }
                match inner_resolved {
                    TypeExpr::Named(name) => format!("{}List", pascal_case(name)),
                    _ => "Array".to_string(),
                }
            }
            TypeExpr::Object(_) => format!("{}Object{}", parent, index),
            TypeExpr::Union(_) => format!("{}Union{}", parent, index),
            TypeExpr::Optional(inner) => {
                let inner_label = self.union_variant_label(parent, inner, index);
                format!("{}Optional", inner_label)
            }
            TypeExpr::Nullable(inner) => {
                let inner_label = self.union_variant_label(parent, inner, index);
                format!("{}Nullable", inner_label)
            }
            TypeExpr::Record(_, _) => "Record".to_string(),
        }
    }

    fn try_emit_literal_enum(&self, name: &str, members: &[TypeExpr]) -> Option<String> {
        let mut cases = Vec::new();
        for member in members {
            let resolved = self.resolve_named_expr(member);
            match resolved {
                TypeExpr::LiteralString(value) => cases.push((value.clone(), "String")),
                TypeExpr::LiteralNumber(value) => cases.push((value.to_string(), "Number")),
                _ => return None,
            }
        }

        if cases.is_empty() {
            return None;
        }

        let is_number = cases.iter().all(|c| c.1 == "Number");
        let raw_type = if is_number { "Double" } else { "String" };

        let mut out = String::new();
        out.push_str(&format!(
            "enum {}: {}, ConvexCodable, Sendable {{\n",
            name, raw_type
        ));
        for (literal, _) in cases {
            let case_name = swift_case_name(&literal);
            out.push_str(&format!(
                "    case {} = {}\n",
                case_name,
                literal_literal(&literal, is_number)
            ));
        }
        out.push_str("}\n");
        Some(out)
    }

    fn emit_discriminated_union(
        &mut self,
        name: &str,
        disc_key: &str,
        variants: &[VariantInfo],
    ) -> Result<()> {
        let mut out = String::new();
        out.push_str(&format!("enum {}: ConvexCodable, Sendable {{\n", name));

        let mut variant_defs = Vec::new();
        for variant in variants {
            let case_name = swift_case_name(&variant.literal);
            let variant_struct = if let Some(name) = &variant.type_name {
                name.clone()
            } else {
                format!("{}{}", name, pascal_case(&variant.literal))
            };
            out.push_str(&format!("    case {}({})\n", case_name, variant_struct));

            if variant.type_name.is_none() {
                let struct_fields = variant.fields.clone();
                self.emit_struct(&variant_struct, &struct_fields)?;
            }
            variant_defs.push(variant_struct);
        }

        out.push_str("\n    private enum CodingKeys: String, CodingKey {\n");
        out.push_str(&format!("        case {}\n", disc_key));
        out.push_str("    }\n\n");

        out.push_str("    init(from decoder: Decoder) throws {\n");
        out.push_str("        let container = try decoder.container(keyedBy: CodingKeys.self)\n");
        out.push_str(&format!(
            "        let tag = try container.decode(String.self, forKey: .{})\n",
            disc_key
        ));
        out.push_str("        switch tag {\n");
        for variant in variants {
            let case_name = swift_case_name(&variant.literal);
            let variant_struct = if let Some(name) = &variant.type_name {
                name.clone()
            } else {
                format!("{}{}", name, pascal_case(&variant.literal))
            };
            out.push_str(&format!(
                "        case {}:\n",
                literal_literal(&variant.literal, false)
            ));
            out.push_str(&format!(
                "            self = .{}(try {}(from: decoder))\n",
                case_name, variant_struct
            ));
        }
        out.push_str("        default:\n");
        out.push_str(&format!(
            "            throw DecodingError.dataCorruptedError(forKey: .{}, in: container, debugDescription: \"Invalid discriminator value\")\n",
            swift_field_name(disc_key)
        ));
        out.push_str("        }\n");
        out.push_str("    }\n");

        out.push_str("\n    func encode(to encoder: Encoder) throws {\n");
        out.push_str("        switch self {\n");
        for variant in variants {
            let case_name = swift_case_name(&variant.literal);
            out.push_str(&format!("        case .{}(let value):\n", case_name));
            out.push_str("            try value.encode(to: encoder)\n");
        }
        out.push_str("        }\n");
        out.push_str("    }\n");

        out.push_str("}\n");
        self.defs.push(out);
        Ok(())
    }

    fn emit_untagged_union(&mut self, name: &str, members: &[TypeExpr]) -> Result<()> {
        let mut out = String::new();
        out.push_str(&format!("enum {}: ConvexCodable, Sendable {{\n", name));

        let mut cases = Vec::new();
        let mut used_cases = std::collections::HashSet::new();
        for (idx, member) in members.iter().enumerate() {
            let label = self.union_variant_label(name, member, idx + 1);
            let mut case_name = lower_camel(&label);
            if case_name.is_empty() {
                case_name = format!("case{}", idx + 1);
            }
            if used_cases.contains(&case_name) {
                case_name = format!("{}{}", case_name, idx + 1);
            }
            used_cases.insert(case_name.clone());
            let case_name = if is_swift_keyword(&case_name) {
                format!("`{}`", case_name)
            } else {
                case_name
            };
            let payload_ty = self.swift_type(member, name, Some(&label))?;
            cases.push((case_name, payload_ty));
        }

        for (case_name, payload_ty) in &cases {
            out.push_str(&format!("    case {}({})\n", case_name, payload_ty));
        }

        out.push_str("\n    init(from decoder: Decoder) throws {\n");
        out.push_str("        let container = try decoder.singleValueContainer()\n");
        for (case_name, payload_ty) in &cases {
            out.push_str(&format!("        if let value = try? container.decode({}.self) {{ self = .{}(value); return }}\n", payload_ty, case_name));
        }
        out.push_str("        throw DecodingError.dataCorruptedError(in: container, debugDescription: \"Invalid union value\")\n");
        out.push_str("    }\n");
        out.push_str("\n    func encode(to encoder: Encoder) throws {\n");
        out.push_str("        switch self {\n");
        for (case_name, _) in &cases {
            out.push_str(&format!("        case .{}(let value):\n", case_name));
            out.push_str("            try value.encode(to: encoder)\n");
        }
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n");

        self.defs.push(out);
        Ok(())
    }

    fn emit_args_impls(&mut self) -> Result<()> {
        let mut seen = HashSet::new();
        for def in &self.function_defs {
            let swift_args = self
                .name_map
                .get(&def.args_key)
                .ok_or_else(|| anyhow!("Missing Swift name for args key {}", def.args_key))?
                .clone();
            if !seen.insert(swift_args.clone()) {
                continue;
            }
            let resolved = self.resolve_named_expr(&def.args_ty);
            let TypeExpr::Object(fields) = resolved else {
                return Err(anyhow!(
                    "Expected args type to be object for {}",
                    swift_args
                ));
            };
            let mut out = String::new();
            out.push_str(&format!("extension {} {{\n", swift_args));
            out.push_str("    func convexArgs() throws -> [String: ConvexEncodable?] {\n");
            if fields.is_empty() {
                out.push_str("        let args: [String: ConvexEncodable?] = [:]\n");
            } else {
                out.push_str("        var args: [String: ConvexEncodable?] = [:]\n");
            }
            for (field_name, field) in fields {
                let (swift_name, _) = swift_member_name(field_name);
                let (is_optional, is_nullable, inner) = self.field_optional_nullable(field);
                let inner_resolved = self.resolve_named_expr(&inner);
                let is_array = matches!(inner_resolved, TypeExpr::Array(_));
                let is_record = matches!(inner_resolved, TypeExpr::Record(_, _));
                if is_optional {
                    out.push_str(&format!("        if let value = {} {{\n", swift_name));
                    if is_array {
                        out.push_str(&format!(
                            "            args[\"{}\"] = value.map {{ $0 as ConvexEncodable? }}\n",
                            field_name
                        ));
                    } else if is_record {
                        out.push_str(&format!(
                            "            args[\"{}\"] = value.mapValues {{ $0 as ConvexEncodable? }}\n",
                            field_name
                        ));
                    } else {
                        out.push_str(&format!("            args[\"{}\"] = value\n", field_name));
                    }
                    out.push_str("        }\n");
                } else if is_nullable {
                    if is_array {
                        out.push_str(&format!(
                            "        args[\"{}\"] = {}?.map {{ $0 as ConvexEncodable? }}\n",
                            field_name, swift_name
                        ));
                    } else if is_record {
                        out.push_str(&format!(
                            "        args[\"{}\"] = {}?.mapValues {{ $0 as ConvexEncodable? }}\n",
                            field_name, swift_name
                        ));
                    } else {
                        out.push_str(&format!(
                            "        args[\"{}\"] = {}\n",
                            field_name, swift_name
                        ));
                    }
                } else if is_array {
                    out.push_str(&format!(
                        "        args[\"{}\"] = {}.map {{ $0 as ConvexEncodable? }}\n",
                        field_name, swift_name
                    ));
                } else if is_record {
                    out.push_str(&format!(
                        "        args[\"{}\"] = {}.mapValues {{ $0 as ConvexEncodable? }}\n",
                        field_name, swift_name
                    ));
                } else {
                    out.push_str(&format!(
                        "        args[\"{}\"] = {}\n",
                        field_name, swift_name
                    ));
                }
            }
            out.push_str("        return args\n");
            out.push_str("    }\n");
            out.push_str("}\n");
            self.args_impls.push(out);
        }
        Ok(())
    }

    fn emit_api(&mut self) -> Result<String> {
        self.emit_args_impls()?;

        let mut out = String::new();
        out.push_str("// swift-format-ignore-file\n");
        out.push_str("// This file is @generated by hull. DO NOT EDIT.\n");
        out.push_str("import Foundation\n");
        out.push_str("@preconcurrency import ConvexMobile\n\n");

        out.push_str("fileprivate func encodeConvexArgs(_ args: [String: ConvexEncodable?]) throws -> [String: String] {\n");
        out.push_str("    try args.mapValues { value in\n");
        out.push_str("        try value?.convexEncode() ?? \"null\"\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");

        out.push_str("fileprivate func convexCall<T: Decodable>(\n");
        out.push_str("    client: MobileConvexClient,\n");
        out.push_str("    path: String,\n");
        out.push_str("    args: [String: ConvexEncodable?],\n");
        out.push_str("    remoteCall: (String, [String: String]) async throws -> String\n");
        out.push_str(") async throws -> T {\n");
        out.push_str("    let encodedArgs = try encodeConvexArgs(args)\n");
        out.push_str("    let rawResult = try await remoteCall(path, encodedArgs)\n");
        out.push_str("    return try JSONDecoder().decode(T.self, from: Data(rawResult.utf8))\n");
        out.push_str("}\n\n");

        out.push_str("fileprivate actor ConvexSubscriptionState {\n");
        out.push_str("    private var handle: SubscriptionHandle?\n");
        out.push_str("    private var terminated = false\n\n");
        out.push_str("    func setHandle(_ handle: SubscriptionHandle) {\n");
        out.push_str("        if terminated {\n");
        out.push_str("            handle.cancel()\n");
        out.push_str("            return\n");
        out.push_str("        }\n");
        out.push_str("        self.handle = handle\n");
        out.push_str("    }\n\n");
        out.push_str("    func terminate() {\n");
        out.push_str("        terminated = true\n");
        out.push_str("        handle?.cancel()\n");
        out.push_str("        handle = nil\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");

        out.push_str("fileprivate struct ConvexSubscriptionError: LocalizedError {\n");
        out.push_str("    let message: String\n");
        out.push_str("    let value: String?\n\n");
        out.push_str("    var errorDescription: String? { message }\n");
        out.push_str("}\n\n");

        out.push_str(
            "fileprivate final class ConvexAsyncQuerySubscriber<T: Decodable>: QuerySubscriber {\n",
        );
        out.push_str("    private let decoder: JSONDecoder\n");
        out.push_str(
            "    private let continuation: AsyncThrowingStream<T, Error>.Continuation\n\n",
        );
        out.push_str("    init(decoder: JSONDecoder, continuation: AsyncThrowingStream<T, Error>.Continuation) {\n");
        out.push_str("        self.decoder = decoder\n");
        out.push_str("        self.continuation = continuation\n");
        out.push_str("    }\n\n");
        out.push_str("    func onError(message: String, value: String?) {\n");
        out.push_str("        continuation.finish(throwing: ConvexSubscriptionError(message: message, value: value))\n");
        out.push_str("    }\n\n");
        out.push_str("    func onUpdate(value: String) {\n");
        out.push_str("        do {\n");
        out.push_str(
            "            let decoded = try decoder.decode(T.self, from: Data(value.utf8))\n",
        );
        out.push_str("            continuation.yield(decoded)\n");
        out.push_str("        } catch {\n");
        out.push_str("            continuation.finish(throwing: error)\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");

        out.push_str("fileprivate func convexSubscribe<T: Decodable>(\n");
        out.push_str("    client: MobileConvexClient,\n");
        out.push_str("    path: String,\n");
        out.push_str("    args: [String: ConvexEncodable?],\n");
        out.push_str("    yielding: T.Type\n");
        out.push_str(") -> AsyncThrowingStream<T, Error> {\n");
        out.push_str("    AsyncThrowingStream { continuation in\n");
        out.push_str("        let state = ConvexSubscriptionState()\n");
        out.push_str("        continuation.onTermination = { _ in\n");
        out.push_str("            Task { await state.terminate() }\n");
        out.push_str("        }\n");
        out.push_str("        let subscriber = ConvexAsyncQuerySubscriber(decoder: JSONDecoder(), continuation: continuation)\n");
        out.push_str("        Task {\n");
        out.push_str("            do {\n");
        out.push_str("                let encodedArgs = try encodeConvexArgs(args)\n");
        out.push_str("                let handle = try await client.subscribe(name: path, args: encodedArgs, subscriber: subscriber)\n");
        out.push_str("                await state.setHandle(handle)\n");
        out.push_str("            } catch {\n");
        out.push_str("                continuation.finish(throwing: error)\n");
        out.push_str("            }\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");

        out.push_str("enum ConvexQueries {\n");
        for def in self.function_defs.clone() {
            if def.kind != ConvexFunctionKind::Query {
                continue;
            }
            let args_name = self
                .name_map
                .get(&def.args_key)
                .cloned()
                .ok_or_else(|| anyhow!("Missing Swift name for args key {}", def.args_key))?;
            let func_name = swift_field_name(&lower_camel(&format!("{}_{}", def.module, def.name)));
            out.push_str(&format!("    static func {}<T: Decodable>(\n", func_name));
            out.push_str("        client: MobileConvexClient,\n");
            out.push_str(&format!("        args: {}\n", args_name));
            out.push_str("    ) -> AsyncThrowingStream<T, Error> {\n");
            out.push_str("        do {\n");
            out.push_str(&format!(
                "            return convexSubscribe(client: client, path: \"{}:{}\", args: try args.convexArgs(), yielding: T.self)\n",
                def.module, def.name
            ));
            out.push_str("        } catch {\n");
            out.push_str("            return AsyncThrowingStream { continuation in\n");
            out.push_str("                continuation.finish(throwing: error)\n");
            out.push_str("            }\n");
            out.push_str("        }\n");
            out.push_str("    }\n\n");

            self.emit_inline_args_wrapper(
                &mut out,
                &def,
                &args_name,
                &func_name,
                InlineWrapperKind::Query,
            )?;
        }
        out.push_str("}\n\n");

        out.push_str("enum ConvexMutations {\n");
        for def in self.function_defs.clone() {
            if def.kind != ConvexFunctionKind::Mutation {
                continue;
            }
            let args_name = self
                .name_map
                .get(&def.args_key)
                .cloned()
                .ok_or_else(|| anyhow!("Missing Swift name for args key {}", def.args_key))?;
            let func_name = swift_field_name(&lower_camel(&format!("{}_{}", def.module, def.name)));
            out.push_str(&format!("    static func {}<T: Decodable>(\n", func_name));
            out.push_str("        client: MobileConvexClient,\n");
            out.push_str(&format!("        args: {}\n", args_name));
            out.push_str("    ) async throws -> T {\n");
            out.push_str(&format!(
                "        try await convexCall(client: client, path: \"{}:{}\", args: try args.convexArgs(), remoteCall: client.mutation)\n",
                def.module, def.name
            ));
            out.push_str("    }\n\n");

            self.emit_inline_args_wrapper(
                &mut out,
                &def,
                &args_name,
                &func_name,
                InlineWrapperKind::Mutation,
            )?;
        }
        out.push_str("}\n\n");

        out.push_str("enum ConvexActions {\n");
        for def in self.function_defs.clone() {
            if def.kind != ConvexFunctionKind::Action {
                continue;
            }
            let args_name = self
                .name_map
                .get(&def.args_key)
                .cloned()
                .ok_or_else(|| anyhow!("Missing Swift name for args key {}", def.args_key))?;
            let func_name = swift_field_name(&lower_camel(&format!("{}_{}", def.module, def.name)));
            out.push_str(&format!("    static func {}<T: Decodable>(\n", func_name));
            out.push_str("        client: MobileConvexClient,\n");
            out.push_str(&format!("        args: {}\n", args_name));
            out.push_str("    ) async throws -> T {\n");
            out.push_str(&format!(
                "        try await convexCall(client: client, path: \"{}:{}\", args: try args.convexArgs(), remoteCall: client.action)\n",
                def.module, def.name
            ));
            out.push_str("    }\n\n");

            self.emit_inline_args_wrapper(
                &mut out,
                &def,
                &args_name,
                &func_name,
                InlineWrapperKind::Action,
            )?;
        }
        out.push_str("}\n");

        Ok(out)
    }

    fn swift_type(&mut self, expr: &TypeExpr, parent: &str, field: Option<&str>) -> Result<String> {
        Ok(match expr {
            TypeExpr::Named(name) => self.swift_name(name)?,
            TypeExpr::String => "String".to_string(),
            TypeExpr::Number => "Double".to_string(),
            TypeExpr::Int64 => "Int64".to_string(),
            TypeExpr::Float64 => "Double".to_string(),
            TypeExpr::Boolean => "Bool".to_string(),
            TypeExpr::Any => "JSONValue".to_string(),
            TypeExpr::Null => "JSONValue".to_string(),
            TypeExpr::LiteralString(_) => "String".to_string(),
            TypeExpr::LiteralNumber(_) => "Double".to_string(),
            TypeExpr::Id(_) => "String".to_string(),
            TypeExpr::Array(inner) => {
                let inner_ty = self.swift_type(inner, parent, field)?;
                format!("[{}]", inner_ty)
            }
            TypeExpr::Record(key, value) => {
                let key_ty = self.swift_type(key, parent, field)?;
                if key_ty != "String" {
                    return Err(anyhow!("Swift only supports String keys for record"));
                }
                let value_ty = self.swift_type(value, parent, field)?;
                format!("[String: {}]", value_ty)
            }
            TypeExpr::Optional(inner) => {
                let inner_ty = self.swift_type(inner, parent, field)?;
                format!("{}?", inner_ty)
            }
            TypeExpr::Nullable(inner) => {
                let inner_ty = self.swift_type(inner, parent, field)?;
                format!("{}?", inner_ty)
            }
            TypeExpr::Object(fields) => {
                let inline_name = inline_type_name(parent, field.unwrap_or("Object"));
                self.emit_struct(&inline_name, fields)?;
                inline_name
            }
            TypeExpr::Union(members) => {
                let inline_name = inline_type_name(parent, field.unwrap_or("Union"));
                self.emit_union(&inline_name, members)?;
                inline_name
            }
        })
    }

    fn swift_type_name(
        &self,
        expr: &TypeExpr,
        parent: &str,
        field: Option<&str>,
    ) -> Result<String> {
        Ok(match expr {
            TypeExpr::Named(name) => self.swift_name(name)?,
            TypeExpr::String => "String".to_string(),
            TypeExpr::Number => "Double".to_string(),
            TypeExpr::Int64 => "Int64".to_string(),
            TypeExpr::Float64 => "Double".to_string(),
            TypeExpr::Boolean => "Bool".to_string(),
            TypeExpr::Any => "JSONValue".to_string(),
            TypeExpr::Null => "JSONValue".to_string(),
            TypeExpr::LiteralString(_) => "String".to_string(),
            TypeExpr::LiteralNumber(_) => "Double".to_string(),
            TypeExpr::Id(_) => "String".to_string(),
            TypeExpr::Array(inner) => {
                let inner_ty = self.swift_type_name(inner, parent, field)?;
                format!("[{}]", inner_ty)
            }
            TypeExpr::Record(key, value) => {
                let key_ty = self.swift_type_name(key, parent, field)?;
                if key_ty != "String" {
                    return Err(anyhow!("Swift only supports String keys for record"));
                }
                let value_ty = self.swift_type_name(value, parent, field)?;
                format!("[String: {}]", value_ty)
            }
            TypeExpr::Optional(inner) => {
                let inner_ty = self.swift_type_name(inner, parent, field)?;
                format!("{}?", inner_ty)
            }
            TypeExpr::Nullable(inner) => {
                let inner_ty = self.swift_type_name(inner, parent, field)?;
                format!("{}?", inner_ty)
            }
            TypeExpr::Object(_) => inline_type_name(parent, field.unwrap_or("Object")),
            TypeExpr::Union(_) => inline_type_name(parent, field.unwrap_or("Union")),
        })
    }

    fn swift_name(&self, raw: &str) -> Result<String> {
        Ok(self
            .name_map
            .get(raw)
            .cloned()
            .unwrap_or_else(|| pascal_case(raw)))
    }

    fn args_fields(&mut self, def: &SwiftFunctionDef, parent: &str) -> Result<Vec<SwiftArgField>> {
        let resolved = self.resolve_named_expr(&def.args_ty).clone();
        let TypeExpr::Object(fields) = resolved else {
            return Err(anyhow!(
                "Expected args type to be object for {}",
                def.args_key
            ));
        };
        let mut out = Vec::new();
        for (field_name, field) in fields {
            let (swift_name, _) = swift_member_name(&field_name);
            let mut swift_type = self.swift_type_name(&field.ty, parent, Some(&field_name))?;
            let (is_optional, is_nullable, _) = self.field_optional_nullable(&field);
            if (is_optional || is_nullable) && !swift_type.ends_with('?') {
                swift_type.push('?');
            }
            out.push(SwiftArgField {
                swift_name,
                swift_type,
                is_optional,
            });
        }
        Ok(out)
    }

    fn emit_inline_args_wrapper(
        &mut self,
        out: &mut String,
        def: &SwiftFunctionDef,
        args_name: &str,
        func_name: &str,
        kind: InlineWrapperKind,
    ) -> Result<()> {
        let fields = self.args_fields(def, args_name)?;
        out.push_str(&format!("    static func {}<T: Decodable>(\n", func_name));
        out.push_str("        client: MobileConvexClient,\n");
        for field in &fields {
            if field.is_optional {
                out.push_str(&format!(
                    "        {}: {} = nil,\n",
                    field.swift_name, field.swift_type
                ));
            } else {
                out.push_str(&format!(
                    "        {}: {},\n",
                    field.swift_name, field.swift_type
                ));
            }
        }
        match kind {
            InlineWrapperKind::Query => {
                out.push_str("    ) -> AsyncThrowingStream<T, Error> {\n");
            }
            InlineWrapperKind::Mutation | InlineWrapperKind::Action => {
                out.push_str("    ) async throws -> T {\n");
            }
        }
        if fields.is_empty() {
            out.push_str(&format!("        let args = {}()\n", args_name));
        } else {
            out.push_str(&format!("        let args = {}(\n", args_name));
            for field in &fields {
                out.push_str(&format!(
                    "            {}: {},\n",
                    field.swift_name, field.swift_name
                ));
            }
            out.push_str("        )\n");
        }
        match kind {
            InlineWrapperKind::Query => {
                out.push_str(&format!(
                    "        return {}(client: client, args: args)\n",
                    func_name
                ));
            }
            InlineWrapperKind::Mutation | InlineWrapperKind::Action => {
                out.push_str(&format!(
                    "        return try await {}(client: client, args: args)\n",
                    func_name
                ));
            }
        }
        out.push_str("    }\n\n");
        Ok(())
    }

    fn resolve_named_expr<'a>(&'a self, expr: &'a TypeExpr) -> &'a TypeExpr {
        match expr {
            TypeExpr::Named(name) => {
                if let Some(inner) = self.types.get(name) {
                    self.resolve_named_expr(inner)
                } else {
                    expr
                }
            }
            _ => expr,
        }
    }

    fn discriminated_union_info(&self, members: &[TypeExpr]) -> Option<(String, Vec<VariantInfo>)> {
        let mut objects = Vec::new();
        for member in members {
            let (type_name, resolved) = match member {
                TypeExpr::Named(name) => {
                    let swift_name = self.name_map.get(name).cloned();
                    (swift_name, self.resolve_named_expr(member))
                }
                _ => (None, self.resolve_named_expr(member)),
            };

            match resolved {
                TypeExpr::Object(fields) => objects.push((type_name, fields.clone())),
                _ => return None,
            }
        }

        if objects.is_empty() {
            return None;
        }

        let mut candidate_keys = Vec::new();
        let first = &objects[0].1;
        for (key, field) in first.iter() {
            if matches!(field.ty, TypeExpr::LiteralString(_)) {
                candidate_keys.push(key.clone());
            }
        }

        let mut discriminator = None;
        for key in candidate_keys {
            let mut all = true;
            for (_, obj) in &objects {
                match obj.get(&key) {
                    Some(field) if matches!(field.ty, TypeExpr::LiteralString(_)) => {}
                    _ => {
                        all = false;
                        break;
                    }
                }
            }
            if all {
                if key == "type" {
                    discriminator = Some(key);
                    break;
                }
                if key == "role" || discriminator.is_none() {
                    discriminator = Some(key);
                }
            }
        }

        let disc_key = discriminator?;
        let mut variants = Vec::new();
        for (type_name, obj) in objects {
            let field = obj.get(&disc_key)?;
            let literal = match &field.ty {
                TypeExpr::LiteralString(value) => value.clone(),
                _ => return None,
            };
            variants.push(VariantInfo {
                literal,
                fields: obj,
                type_name,
            });
        }

        Some((disc_key, variants))
    }

    fn field_optional_nullable(&self, field: &Field) -> (bool, bool, TypeExpr) {
        let mut is_optional = field.optional;
        let mut is_nullable = field.nullable;
        let mut inner = field.ty.clone();

        loop {
            match inner {
                TypeExpr::Optional(inner_expr) => {
                    is_optional = true;
                    inner = *inner_expr;
                    continue;
                }
                TypeExpr::Nullable(inner_expr) => {
                    is_nullable = true;
                    inner = *inner_expr;
                    continue;
                }
                TypeExpr::Named(ref name) => {
                    if let Some(resolved) = self.types.get(name).cloned() {
                        if matches!(resolved, TypeExpr::Optional(_) | TypeExpr::Nullable(_)) {
                            inner = resolved;
                            continue;
                        }
                    }
                }
                _ => {}
            }
            break;
        }

        (is_optional, is_nullable, inner)
    }
}

struct RustGenerator {
    used_names: HashSet<String>,
    defs: Vec<String>,
    convex_impls: Vec<String>,
    args_impls: Vec<String>,
    validate_impls: Vec<String>,
    name_map: HashMap<String, String>,
    types: HashMap<String, TypeExpr>,
    args_types: Vec<String>,
    convex_value_path: String,
}

impl RustGenerator {
    fn new(
        types: &IndexMap<String, TypeExpr>,
        name_map: HashMap<String, String>,
        args_types: Vec<String>,
        convex_value_path: String,
    ) -> Self {
        let types_map = types.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        Self {
            used_names: HashSet::new(),
            defs: Vec::new(),
            convex_impls: Vec::new(),
            args_impls: Vec::new(),
            validate_impls: Vec::new(),
            name_map,
            types: types_map,
            args_types,
            convex_value_path,
        }
    }

    fn generate(&mut self) -> Result<String> {
        let mut names: Vec<_> = self.types.keys().cloned().collect();
        names.sort();

        for name in names {
            let expr = self.types.get(&name).cloned().unwrap();
            let rust_name = self.name_map.get(&name).unwrap().clone();
            self.emit_named(&rust_name, &expr)?;
        }
        self.emit_args_impls();

        let mut out = String::new();
        out.push_str("// This file is @generated by hull. DO NOT EDIT.\n");
        out.push_str("#![allow(non_snake_case)]\n");
        out.push_str("#![allow(clippy::disallowed_types)]\n");
        out.push_str("use serde::{Deserialize, Serialize};\n");
        out.push_str("use serde_json::Value as JsonValue;\n");
        out.push_str("use std::collections::{BTreeMap, HashMap};\n");
        out.push_str(&format!("use {};\n\n", self.convex_value_path));

        out.push_str("#[derive(Debug, Clone)]\n");
        out.push_str("pub struct ConvexTypeError {\n");
        out.push_str("    message: String,\n");
        out.push_str("}\n");
        out.push_str("impl ConvexTypeError {\n");
        out.push_str("    pub fn new(message: impl Into<String>) -> Self {\n");
        out.push_str("        Self {\n");
        out.push_str("            message: message.into(),\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl std::fmt::Display for ConvexTypeError {\n");
        out.push_str("    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {\n");
        out.push_str("        write!(f, \"{}\", self.message)\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl std::error::Error for ConvexTypeError {}\n\n");

        out.push_str("pub trait ConvexEncode {\n");
        out.push_str("    fn to_convex_value(&self) -> ConvexValue;\n");
        out.push_str("}\n\n");
        out.push_str("mod sealed {\n");
        out.push_str("    pub trait Sealed {}\n");
        out.push_str("}\n\n");
        out.push_str("pub trait ConvexArgs: sealed::Sealed + ConvexEncode {}\n");
        out.push_str("impl<T> ConvexArgs for T\n");
        out.push_str("where\n");
        out.push_str("    T: sealed::Sealed + ConvexEncode,\n");
        out.push_str("{}\n\n");
        out.push_str("pub trait JsonValidate: Sized {\n");
        out.push_str("    fn validate_json(value: &JsonValue) -> Result<(), ConvexTypeError>;\n");
        out.push_str("    fn parse_json(value: &JsonValue) -> Result<Self, ConvexTypeError>\n");
        out.push_str("    where\n");
        out.push_str("        Self: for<'de> Deserialize<'de>,\n");
        out.push_str("    {\n");
        out.push_str("        Self::validate_json(value)?;\n");
        out.push_str("        serde_json::from_value(value.clone())\n");
        out.push_str("            .map_err(|err| ConvexTypeError::new(err.to_string()))\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");

        out.push_str("fn json_value_to_convex(value: &JsonValue) -> ConvexValue {\n");
        out.push_str("    match value {\n");
        out.push_str("        JsonValue::Null => ConvexValue::Null,\n");
        out.push_str("        JsonValue::Bool(value) => ConvexValue::Bool(*value),\n");
        out.push_str("        JsonValue::Number(value) => ConvexValue::Float(value.as_f64().unwrap_or(0.0)),\n");
        out.push_str("        JsonValue::String(value) => ConvexValue::String(value.clone()),\n");
        out.push_str("        JsonValue::Array(values) => {\n");
        out.push_str(
            "            ConvexValue::Array(values.iter().map(json_value_to_convex).collect())\n",
        );
        out.push_str("        }\n");
        out.push_str("        JsonValue::Object(values) => ConvexValue::Object(\n");
        out.push_str("            values\n");
        out.push_str("                .iter()\n");
        out.push_str(
            "                .map(|(key, value)| (key.clone(), json_value_to_convex(value)))\n",
        );
        out.push_str("                .collect(),\n");
        out.push_str("        ),\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");

        out.push_str("impl ConvexEncode for String {\n");
        out.push_str("    fn to_convex_value(&self) -> ConvexValue {\n");
        out.push_str("        ConvexValue::String(self.clone())\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl ConvexEncode for bool {\n");
        out.push_str("    fn to_convex_value(&self) -> ConvexValue {\n");
        out.push_str("        ConvexValue::Bool(*self)\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl ConvexEncode for f64 {\n");
        out.push_str("    fn to_convex_value(&self) -> ConvexValue {\n");
        out.push_str("        ConvexValue::Float(*self)\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl ConvexEncode for i64 {\n");
        out.push_str("    fn to_convex_value(&self) -> ConvexValue {\n");
        out.push_str("        ConvexValue::Int(*self)\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl ConvexEncode for () {\n");
        out.push_str("    fn to_convex_value(&self) -> ConvexValue {\n");
        out.push_str("        ConvexValue::Null\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl ConvexEncode for JsonValue {\n");
        out.push_str("    fn to_convex_value(&self) -> ConvexValue {\n");
        out.push_str("        json_value_to_convex(self)\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl<T> ConvexEncode for Option<T>\n");
        out.push_str("where\n");
        out.push_str("    T: ConvexEncode,\n");
        out.push_str("{\n");
        out.push_str("    fn to_convex_value(&self) -> ConvexValue {\n");
        out.push_str("        match self {\n");
        out.push_str("            Some(value) => value.to_convex_value(),\n");
        out.push_str("            None => ConvexValue::Null,\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl<T> ConvexEncode for Vec<T>\n");
        out.push_str("where\n");
        out.push_str("    T: ConvexEncode,\n");
        out.push_str("{\n");
        out.push_str("    fn to_convex_value(&self) -> ConvexValue {\n");
        out.push_str("        ConvexValue::Array(self.iter().map(ConvexEncode::to_convex_value).collect())\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl<T> ConvexEncode for HashMap<String, T>\n");
        out.push_str("where\n");
        out.push_str("    T: ConvexEncode,\n");
        out.push_str("{\n");
        out.push_str("    fn to_convex_value(&self) -> ConvexValue {\n");
        out.push_str("        let mut map = BTreeMap::new();\n");
        out.push_str("        for (key, value) in self {\n");
        out.push_str("            map.insert(key.clone(), value.to_convex_value());\n");
        out.push_str("        }\n");
        out.push_str("        ConvexValue::Object(map)\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");

        out.push_str("impl JsonValidate for String {\n");
        out.push_str("    fn validate_json(value: &JsonValue) -> Result<(), ConvexTypeError> {\n");
        out.push_str("        if value.is_string() {\n");
        out.push_str("            Ok(())\n");
        out.push_str("        } else {\n");
        out.push_str("            Err(ConvexTypeError::new(\"expected string\"))\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl JsonValidate for bool {\n");
        out.push_str("    fn validate_json(value: &JsonValue) -> Result<(), ConvexTypeError> {\n");
        out.push_str("        if value.is_boolean() {\n");
        out.push_str("            Ok(())\n");
        out.push_str("        } else {\n");
        out.push_str("            Err(ConvexTypeError::new(\"expected bool\"))\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl JsonValidate for f64 {\n");
        out.push_str("    fn validate_json(value: &JsonValue) -> Result<(), ConvexTypeError> {\n");
        out.push_str("        if value.is_number() {\n");
        out.push_str("            Ok(())\n");
        out.push_str("        } else {\n");
        out.push_str("            Err(ConvexTypeError::new(\"expected number\"))\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl JsonValidate for i64 {\n");
        out.push_str("    fn validate_json(value: &JsonValue) -> Result<(), ConvexTypeError> {\n");
        out.push_str("        if value.as_i64().is_some() {\n");
        out.push_str("            Ok(())\n");
        out.push_str("        } else {\n");
        out.push_str("            Err(ConvexTypeError::new(\"expected int64\"))\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl JsonValidate for () {\n");
        out.push_str("    fn validate_json(value: &JsonValue) -> Result<(), ConvexTypeError> {\n");
        out.push_str("        if value.is_null() {\n");
        out.push_str("            Ok(())\n");
        out.push_str("        } else {\n");
        out.push_str("            Err(ConvexTypeError::new(\"expected null\"))\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl JsonValidate for JsonValue {\n");
        out.push_str("    fn validate_json(_value: &JsonValue) -> Result<(), ConvexTypeError> {\n");
        out.push_str("        Ok(())\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl<T> JsonValidate for Option<T>\n");
        out.push_str("where\n");
        out.push_str("    T: JsonValidate,\n");
        out.push_str("{\n");
        out.push_str("    fn validate_json(value: &JsonValue) -> Result<(), ConvexTypeError> {\n");
        out.push_str("        if value.is_null() {\n");
        out.push_str("            Ok(())\n");
        out.push_str("        } else {\n");
        out.push_str("            T::validate_json(value)\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl<T> JsonValidate for Vec<T>\n");
        out.push_str("where\n");
        out.push_str("    T: JsonValidate,\n");
        out.push_str("{\n");
        out.push_str("    fn validate_json(value: &JsonValue) -> Result<(), ConvexTypeError> {\n");
        out.push_str("        let array = value\n");
        out.push_str("            .as_array()\n");
        out.push_str("            .ok_or_else(|| ConvexTypeError::new(\"expected array\"))?;\n");
        out.push_str("        for item in array {\n");
        out.push_str("            T::validate_json(item)?;\n");
        out.push_str("        }\n");
        out.push_str("        Ok(())\n");
        out.push_str("    }\n");
        out.push_str("}\n");
        out.push_str("impl<T> JsonValidate for HashMap<String, T>\n");
        out.push_str("where\n");
        out.push_str("    T: JsonValidate,\n");
        out.push_str("{\n");
        out.push_str("    fn validate_json(value: &JsonValue) -> Result<(), ConvexTypeError> {\n");
        out.push_str("        let object = value\n");
        out.push_str("            .as_object()\n");
        out.push_str("            .ok_or_else(|| ConvexTypeError::new(\"expected object\"))?;\n");
        out.push_str("        for item in object.values() {\n");
        out.push_str("            T::validate_json(item)?;\n");
        out.push_str("        }\n");
        out.push_str("        Ok(())\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");

        for def in &self.defs {
            out.push_str(def);
            out.push('\n');
        }

        for def in &self.convex_impls {
            out.push_str(def);
            out.push('\n');
        }

        for def in &self.args_impls {
            out.push_str(def);
            out.push('\n');
        }

        for def in &self.validate_impls {
            out.push_str(def);
            out.push('\n');
        }

        Ok(out)
    }

    fn emit_args_impls(&mut self) {
        let mut seen = HashSet::new();
        for name in &self.args_types {
            if !seen.insert(name.clone()) {
                continue;
            }
            let mut out = String::new();
            out.push_str(&format!("impl sealed::Sealed for {} {{}}\n", name));
            self.args_impls.push(out);
        }
    }

    fn emit_named(&mut self, name: &str, ty: &TypeExpr) -> Result<()> {
        if self.used_names.contains(name) {
            return Ok(());
        }
        self.used_names.insert(name.to_string());
        match ty {
            TypeExpr::Named(alias) => {
                let rust_name = self.rust_name(alias)?;
                self.defs
                    .push(format!("pub type {} = {};", name, rust_name));
                Ok(())
            }
            TypeExpr::Object(fields) => self.emit_struct(name, fields),
            TypeExpr::Union(members) => self.emit_union(name, members),
            TypeExpr::Array(inner) => {
                let inner_ty = self.rust_type(inner, name, None)?;
                self.defs
                    .push(format!("pub type {} = Vec<{}>;", name, inner_ty));
                Ok(())
            }
            TypeExpr::Nullable(inner) => {
                let inner_ty = self.rust_type(inner, name, None)?;
                self.defs
                    .push(format!("pub type {} = Option<{}>;", name, inner_ty));
                Ok(())
            }
            TypeExpr::Record(key, value) => {
                let key_ty = self.rust_type(key, name, Some("Key"))?;
                if key_ty != "String" {
                    return Err(anyhow!("Rust only supports String keys for record"));
                }
                let value_ty = self.rust_type(value, name, Some("Value"))?;
                self.defs.push(format!(
                    "pub type {} = HashMap<String, {}>;",
                    name, value_ty
                ));
                Ok(())
            }
            TypeExpr::Optional(inner) => {
                let inner_ty = self.rust_type(inner, name, None)?;
                self.defs
                    .push(format!("pub type {} = Option<{}>;", name, inner_ty));
                Ok(())
            }
            TypeExpr::LiteralString(_) | TypeExpr::LiteralNumber(_) => {
                Err(anyhow!("Top-level literal type not supported: {}", name))
            }
            TypeExpr::String => {
                self.defs.push(format!("pub type {} = String;", name));
                Ok(())
            }
            TypeExpr::Number => {
                self.defs.push(format!("pub type {} = f64;", name));
                Ok(())
            }
            TypeExpr::Int64 => {
                self.defs.push(format!("pub type {} = i64;", name));
                Ok(())
            }
            TypeExpr::Float64 => {
                self.defs.push(format!("pub type {} = f64;", name));
                Ok(())
            }
            TypeExpr::Boolean => {
                self.defs.push(format!("pub type {} = bool;", name));
                Ok(())
            }
            TypeExpr::Any => {
                self.defs.push(format!("pub type {} = JsonValue;", name));
                Ok(())
            }
            TypeExpr::Null => {
                self.defs.push(format!("pub type {} = ();", name));
                Ok(())
            }
            TypeExpr::Id(_) => {
                self.defs.push(format!("pub type {} = String;", name));
                Ok(())
            }
        }
    }

    fn emit_struct(&mut self, name: &str, fields: &BTreeMap<String, Field>) -> Result<()> {
        let mut out = String::new();
        out.push_str("#[derive(Clone, Debug, Serialize, Deserialize)]\n");
        out.push_str(&format!("pub struct {} {{\n", name));

        for (field_name, field) in fields {
            let mut ty = self.rust_type(&field.ty, name, Some(field_name))?;
            if field.optional || field.nullable {
                ty = format!("Option<{}>", ty);
            }
            let (rust_name, rename) = rust_field_name(field_name);
            if let Some(original) = rename {
                out.push_str(&format!("    #[serde(rename = \"{}\")]\n", original));
            }
            out.push_str(&format!("    pub {}: {},\n", rust_name, ty));
        }

        out.push_str("}\n");
        self.defs.push(out);

        let mut convex_impl = String::new();
        convex_impl.push_str(&format!("impl ConvexEncode for {} {{\n", name));
        convex_impl.push_str("    fn to_convex_value(&self) -> ConvexValue {\n");
        if fields.is_empty() {
            convex_impl.push_str("        let map = BTreeMap::new();\n");
        } else {
            convex_impl.push_str("        let mut map = BTreeMap::new();\n");
        }

        for (field_name, field) in fields {
            let (rust_name, _) = rust_field_name(field_name);
            let (is_optional, is_nullable, _) = self.field_optional_inner(field);
            if is_optional {
                convex_impl.push_str(&format!(
                    "        if let Some(value) = &self.{} {{\n",
                    rust_name
                ));
                convex_impl.push_str(&format!(
                    "            map.insert(\"{}\".to_string(), value.to_convex_value());\n",
                    field_name
                ));
                convex_impl.push_str("        }\n");
            } else if is_nullable {
                convex_impl.push_str(&format!("        match &self.{} {{\n", rust_name));
                convex_impl.push_str(&format!(
                    "            Some(value) => {{ map.insert(\"{}\".to_string(), value.to_convex_value()); }}\n",
                    field_name
                ));
                convex_impl.push_str(&format!(
                    "            None => {{ map.insert(\"{}\".to_string(), ConvexValue::Null); }}\n",
                    field_name
                ));
                convex_impl.push_str("        }\n");
            } else {
                convex_impl.push_str(&format!(
                    "        map.insert(\"{}\".to_string(), self.{}.to_convex_value());\n",
                    field_name, rust_name
                ));
            }
        }

        convex_impl.push_str("        ConvexValue::Object(map)\n");
        convex_impl.push_str("    }\n");
        convex_impl.push_str("}\n");
        self.convex_impls.push(convex_impl);

        let mut validate_impl = String::new();
        validate_impl.push_str(&format!("impl JsonValidate for {} {{\n", name));
        validate_impl
            .push_str("    fn validate_json(value: &JsonValue) -> Result<(), ConvexTypeError> {\n");
        validate_impl.push_str("        let object = value\n");
        validate_impl.push_str("            .as_object()\n");
        validate_impl
            .push_str("            .ok_or_else(|| ConvexTypeError::new(\"expected object\"))?;\n");
        if fields.is_empty() {
            validate_impl.push_str("        if let Some(key) = object.keys().next() {\n");
            validate_impl.push_str(
                "            return Err(ConvexTypeError::new(format!(\"unknown field: {}\", key)));\n",
            );
            validate_impl.push_str("        }\n");
        } else {
            validate_impl.push_str("        for key in object.keys() {\n");
            validate_impl.push_str("            match key.as_str() {\n");
            for field_name in fields.keys() {
                validate_impl.push_str(&format!("                \"{}\" => {{}}\n", field_name));
            }
            validate_impl.push_str(
                "                _ => return Err(ConvexTypeError::new(format!(\"unknown field: {}\", key))),\n",
            );
            validate_impl.push_str("            }\n");
            validate_impl.push_str("        }\n");
        }

        for (field_name, field) in fields {
            let (is_optional, is_nullable, inner_expr) = self.field_optional_inner(field);
            let inner_type = self.rust_type_name(&inner_expr, name, Some(field_name))?;
            validate_impl.push_str(&format!(
                "        if let Some(field_value) = object.get(\"{}\") {{\n",
                field_name
            ));
            if is_optional && !is_nullable {
                validate_impl.push_str("            if field_value.is_null() {\n");
                validate_impl.push_str(&format!(
                    "                return Err(ConvexTypeError::new(\"optional field '{}' must be omitted, not null\"));\n",
                    field_name
                ));
                validate_impl.push_str("            }\n");
            }
            if is_nullable {
                validate_impl.push_str("            if !field_value.is_null() {\n");
                validate_impl.push_str(&format!(
                    "                <{} as JsonValidate>::validate_json(field_value)?;\n",
                    inner_type
                ));
                validate_impl.push_str("            }\n");
            } else {
                validate_impl.push_str(&format!(
                    "            <{} as JsonValidate>::validate_json(field_value)?;\n",
                    inner_type
                ));
            }
            if !is_optional {
                validate_impl.push_str("        } else {\n");
                validate_impl.push_str(&format!(
                    "            return Err(ConvexTypeError::new(\"missing required field: {}\"));\n",
                    field_name
                ));
                validate_impl.push_str("        }\n");
            } else {
                validate_impl.push_str("        }\n");
            }
        }

        validate_impl.push_str("        Ok(())\n");
        validate_impl.push_str("    }\n");
        validate_impl.push_str("}\n");
        self.validate_impls.push(validate_impl);
        Ok(())
    }

    fn emit_union(&mut self, name: &str, members: &[TypeExpr]) -> Result<()> {
        if let Some(info) = self.try_emit_literal_enum(name, members) {
            self.defs.push(info.enum_def);
            self.emit_literal_enum_impls(name, &info.literals);
            return Ok(());
        }

        if let Some((disc_key, variants)) = self.discriminated_union_info(members) {
            return self.emit_discriminated_union(name, &disc_key, &variants);
        }

        self.emit_untagged_union(name, members)
    }

    fn try_emit_literal_enum(&self, name: &str, members: &[TypeExpr]) -> Option<LiteralEnumInfo> {
        let mut cases = Vec::new();
        for member in members {
            let resolved = self.resolve_named_expr(member);
            match resolved {
                TypeExpr::LiteralString(value) => cases.push(value.clone()),
                _ => return None,
            }
        }
        if cases.is_empty() {
            return None;
        }
        let mut out = String::new();
        out.push_str("#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]\n");
        out.push_str(&format!("pub enum {} {{\n", name));
        for literal in &cases {
            let variant = rust_variant_name(literal);
            out.push_str(&format!("    #[serde(rename = \"{}\")]\n", literal));
            out.push_str(&format!("    {},\n", variant));
        }
        out.push_str("}\n");
        Some(LiteralEnumInfo {
            enum_def: out,
            literals: cases,
        })
    }

    fn emit_discriminated_union(
        &mut self,
        name: &str,
        disc_key: &str,
        variants: &[VariantInfo],
    ) -> Result<()> {
        for variant in variants {
            let variant_struct = format!(
                "{}{}",
                name,
                variant
                    .type_name
                    .clone()
                    .unwrap_or_else(|| rust_variant_name(&variant.literal))
            );
            let mut fields = variant.fields.clone();
            fields.remove(disc_key);
            self.emit_struct(&variant_struct, &fields)?;
        }

        let mut out = String::new();
        out.push_str("#[derive(Clone, Debug, Serialize, Deserialize)]\n");
        out.push_str(&format!("#[serde(tag = \"{}\")]\n", disc_key));
        out.push_str(&format!("pub enum {} {{\n", name));
        for variant in variants {
            let variant_struct = format!(
                "{}{}",
                name,
                variant
                    .type_name
                    .clone()
                    .unwrap_or_else(|| rust_variant_name(&variant.literal))
            );
            let variant_name = rust_variant_name(&variant.literal);
            out.push_str(&format!("    #[serde(rename = \"{}\")]\n", variant.literal));
            out.push_str(&format!("    {}({}),\n", variant_name, variant_struct));
        }
        out.push_str("}\n");
        self.defs.push(out);
        self.emit_discriminated_union_impls(name, disc_key, variants)?;
        Ok(())
    }

    fn emit_untagged_union(&mut self, name: &str, members: &[TypeExpr]) -> Result<()> {
        let mut out = String::new();
        out.push_str("#[derive(Clone, Debug, Serialize, Deserialize)]\n");
        out.push_str("#[serde(untagged)]\n");
        out.push_str(&format!("pub enum {} {{\n", name));

        let mut used_cases = HashSet::new();
        let mut variant_info = Vec::new();
        for (idx, member) in members.iter().enumerate() {
            let label = self.union_variant_label(name, member, idx + 1);
            let mut variant_name = pascal_case(&label);
            if variant_name.is_empty() {
                variant_name = format!("Variant{}", idx + 1);
            }
            if used_cases.contains(&variant_name) {
                variant_name = format!("{}{}", variant_name, idx + 1);
            }
            used_cases.insert(variant_name.clone());
            let payload_ty = self.rust_type(member, name, Some(&label))?;
            out.push_str(&format!("    {}({}),\n", variant_name, payload_ty));
            variant_info.push((variant_name, member.clone()));
        }

        out.push_str("}\n");
        self.defs.push(out);
        self.emit_untagged_union_impls(name, &variant_info)?;
        Ok(())
    }

    fn emit_literal_enum_impls(&mut self, name: &str, literals: &[String]) {
        let mut convex_impl = String::new();
        convex_impl.push_str(&format!("impl ConvexEncode for {} {{\n", name));
        convex_impl.push_str("    fn to_convex_value(&self) -> ConvexValue {\n");
        convex_impl.push_str("        match self {\n");
        for literal in literals {
            let variant = rust_variant_name(literal);
            convex_impl.push_str(&format!(
                "            {}::{} => ConvexValue::String(\"{}\".to_string()),\n",
                name, variant, literal
            ));
        }
        convex_impl.push_str("        }\n");
        convex_impl.push_str("    }\n");
        convex_impl.push_str("}\n");
        self.convex_impls.push(convex_impl);

        let mut validate_impl = String::new();
        validate_impl.push_str(&format!("impl JsonValidate for {} {{\n", name));
        validate_impl
            .push_str("    fn validate_json(value: &JsonValue) -> Result<(), ConvexTypeError> {\n");
        validate_impl.push_str("        let value = value.as_str().ok_or_else(|| ConvexTypeError::new(\"expected string\"))?;\n");
        validate_impl.push_str("        match value {\n");
        for literal in literals {
            validate_impl.push_str(&format!("            \"{}\" => Ok(()),\n", literal));
        }
        validate_impl
            .push_str("            _ => Err(ConvexTypeError::new(\"invalid enum value\")),\n");
        validate_impl.push_str("        }\n");
        validate_impl.push_str("    }\n");
        validate_impl.push_str("}\n");
        self.validate_impls.push(validate_impl);
    }

    fn emit_discriminated_union_impls(
        &mut self,
        name: &str,
        disc_key: &str,
        variants: &[VariantInfo],
    ) -> Result<()> {
        let mut convex_impl = String::new();
        convex_impl.push_str(&format!("impl ConvexEncode for {} {{\n", name));
        convex_impl.push_str("    fn to_convex_value(&self) -> ConvexValue {\n");
        convex_impl.push_str("        match self {\n");
        for variant in variants {
            let variant_name = rust_variant_name(&variant.literal);
            let mut fields = variant.fields.clone();
            fields.remove(disc_key);
            let pattern = if fields.is_empty() {
                format!("{}::{}(_)", name, variant_name)
            } else {
                format!("{}::{}(data)", name, variant_name)
            };
            convex_impl.push_str(&format!("            {} => {{\n", pattern));
            convex_impl.push_str("                let mut map = BTreeMap::new();\n");
            convex_impl.push_str(&format!(
                "                map.insert(\"{}\".to_string(), ConvexValue::String(\"{}\".to_string()));\n",
                disc_key, variant.literal
            ));
            for (field_name, field) in fields {
                let (rust_name, _) = rust_field_name(&field_name);
                let (is_optional, is_nullable, _) = self.field_optional_inner(&field);
                if is_optional {
                    convex_impl.push_str(&format!(
                        "                if let Some(value) = &data.{} {{\n",
                        rust_name
                    ));
                    convex_impl.push_str(&format!(
                        "                    map.insert(\"{}\".to_string(), value.to_convex_value());\n",
                        field_name
                    ));
                    convex_impl.push_str("                }\n");
                } else if is_nullable {
                    convex_impl
                        .push_str(&format!("                match &data.{} {{\n", rust_name));
                    convex_impl.push_str(&format!(
                        "                    Some(value) => {{ map.insert(\"{}\".to_string(), value.to_convex_value()); }}\n",
                        field_name
                    ));
                    convex_impl.push_str(&format!(
                        "                    None => {{ map.insert(\"{}\".to_string(), ConvexValue::Null); }}\n",
                        field_name
                    ));
                    convex_impl.push_str("                }\n");
                } else {
                    convex_impl.push_str(&format!(
                        "                map.insert(\"{}\".to_string(), data.{}.to_convex_value());\n",
                        field_name, rust_name
                    ));
                }
            }
            convex_impl.push_str("                ConvexValue::Object(map)\n");
            convex_impl.push_str("            }\n");
        }
        convex_impl.push_str("        }\n");
        convex_impl.push_str("    }\n");
        convex_impl.push_str("}\n");
        self.convex_impls.push(convex_impl);

        let mut validate_impl = String::new();
        validate_impl.push_str(&format!("impl JsonValidate for {} {{\n", name));
        validate_impl
            .push_str("    fn validate_json(value: &JsonValue) -> Result<(), ConvexTypeError> {\n");
        validate_impl.push_str("        let object = value\n");
        validate_impl.push_str("            .as_object()\n");
        validate_impl
            .push_str("            .ok_or_else(|| ConvexTypeError::new(\"expected object\"))?;\n");
        validate_impl.push_str("        let tag = object\n");
        validate_impl.push_str(&format!("            .get(\"{}\")\n", disc_key));
        validate_impl.push_str("            .and_then(JsonValue::as_str)\n");
        validate_impl.push_str(
            "            .ok_or_else(|| ConvexTypeError::new(\"missing discriminator\"))?;\n",
        );
        validate_impl.push_str("        match tag {\n");
        for variant in variants {
            let variant_struct_name = format!(
                "{}{}",
                name,
                variant
                    .type_name
                    .clone()
                    .unwrap_or_else(|| rust_variant_name(&variant.literal))
            );
            let mut fields = variant.fields.clone();
            fields.remove(disc_key);
            validate_impl.push_str(&format!("            \"{}\" => {{\n", variant.literal));
            validate_impl.push_str("                for key in object.keys() {\n");
            validate_impl.push_str("                    match key.as_str() {\n");
            validate_impl.push_str(&format!(
                "                        \"{}\" => {{}}\n",
                disc_key
            ));
            for field_name in fields.keys() {
                validate_impl.push_str(&format!(
                    "                        \"{}\" => {{}}\n",
                    field_name
                ));
            }
            validate_impl.push_str(
                "                        _ => return Err(ConvexTypeError::new(format!(\"unknown field: {}\", key))),\n",
            );
            validate_impl.push_str("                    }\n");
            validate_impl.push_str("                }\n");
            for (field_name, field) in &fields {
                let (is_optional, is_nullable, inner_expr) = self.field_optional_inner(field);
                let inner_type =
                    self.rust_type_name(&inner_expr, &variant_struct_name, Some(field_name))?;
                validate_impl.push_str(&format!(
                    "                if let Some(field_value) = object.get(\"{}\") {{\n",
                    field_name
                ));
                if is_optional && !is_nullable {
                    validate_impl.push_str("                    if field_value.is_null() {\n");
                    validate_impl.push_str(&format!(
                        "                        return Err(ConvexTypeError::new(\"optional field '{}' must be omitted, not null\"));\n",
                        field_name
                    ));
                    validate_impl.push_str("                    }\n");
                }
                if is_nullable {
                    validate_impl.push_str("                    if !field_value.is_null() {\n");
                    validate_impl.push_str(&format!(
                        "                        <{} as JsonValidate>::validate_json(field_value)?;\n",
                        inner_type
                    ));
                    validate_impl.push_str("                    }\n");
                } else {
                    validate_impl.push_str(&format!(
                        "                    <{} as JsonValidate>::validate_json(field_value)?;\n",
                        inner_type
                    ));
                }
                if !is_optional {
                    validate_impl.push_str("                } else {\n");
                    validate_impl.push_str(&format!(
                        "                    return Err(ConvexTypeError::new(\"missing required field: {}\"));\n",
                        field_name
                    ));
                    validate_impl.push_str("                }\n");
                } else {
                    validate_impl.push_str("                }\n");
                }
            }
            validate_impl.push_str("                Ok(())\n");
            validate_impl.push_str("            }\n");
        }
        validate_impl
            .push_str("            _ => Err(ConvexTypeError::new(\"unknown discriminator\")),\n");
        validate_impl.push_str("        }\n");
        validate_impl.push_str("    }\n");
        validate_impl.push_str("}\n");
        self.validate_impls.push(validate_impl);
        Ok(())
    }

    fn emit_untagged_union_impls(
        &mut self,
        name: &str,
        variants: &[(String, TypeExpr)],
    ) -> Result<()> {
        let mut convex_impl = String::new();
        convex_impl.push_str(&format!("impl ConvexEncode for {} {{\n", name));
        convex_impl.push_str("    fn to_convex_value(&self) -> ConvexValue {\n");
        convex_impl.push_str("        match self {\n");
        for (variant_name, member) in variants {
            let payload_ty = self.rust_type_name(member, name, Some(variant_name))?;
            convex_impl.push_str(&format!(
                "            {}::{}(data) => <{} as ConvexEncode>::to_convex_value(data),\n",
                name, variant_name, payload_ty
            ));
        }
        convex_impl.push_str("        }\n");
        convex_impl.push_str("    }\n");
        convex_impl.push_str("}\n");
        self.convex_impls.push(convex_impl);

        let mut validate_impl = String::new();
        validate_impl.push_str(&format!("impl JsonValidate for {} {{\n", name));
        validate_impl
            .push_str("    fn validate_json(value: &JsonValue) -> Result<(), ConvexTypeError> {\n");
        validate_impl.push_str("        let mut matches = 0;\n");
        for (variant_name, member) in variants {
            let payload_ty = self.rust_type_name(member, name, Some(variant_name))?;
            validate_impl.push_str(&format!(
                "        if <{} as JsonValidate>::validate_json(value).is_ok() {{\n",
                payload_ty
            ));
            validate_impl.push_str("            matches += 1;\n");
            validate_impl.push_str("        }\n");
        }
        validate_impl.push_str("        if matches == 1 {\n");
        validate_impl.push_str("            Ok(())\n");
        validate_impl.push_str("        } else if matches == 0 {\n");
        validate_impl
            .push_str("            Err(ConvexTypeError::new(\"no matching union variant\"))\n");
        validate_impl.push_str("        } else {\n");
        validate_impl
            .push_str("            Err(ConvexTypeError::new(\"ambiguous union variant\"))\n");
        validate_impl.push_str("        }\n");
        validate_impl.push_str("    }\n");
        validate_impl.push_str("}\n");
        self.validate_impls.push(validate_impl);
        Ok(())
    }

    fn rust_type(&mut self, expr: &TypeExpr, parent: &str, field: Option<&str>) -> Result<String> {
        Ok(match expr {
            TypeExpr::Named(name) => self.rust_name(name)?,
            TypeExpr::String => "String".to_string(),
            TypeExpr::Number => "f64".to_string(),
            TypeExpr::Int64 => "i64".to_string(),
            TypeExpr::Float64 => "f64".to_string(),
            TypeExpr::Boolean => "bool".to_string(),
            TypeExpr::Any => "JsonValue".to_string(),
            TypeExpr::Null => "()".to_string(),
            TypeExpr::LiteralString(_) => "String".to_string(),
            TypeExpr::LiteralNumber(_) => "f64".to_string(),
            TypeExpr::Id(_) => "String".to_string(),
            TypeExpr::Array(inner) => {
                let inner_ty = self.rust_type(inner, parent, field)?;
                format!("Vec<{}>", inner_ty)
            }
            TypeExpr::Record(key, value) => {
                let key_ty = self.rust_type(key, parent, field)?;
                if key_ty != "String" {
                    return Err(anyhow!("Rust only supports String keys for record"));
                }
                let value_ty = self.rust_type(value, parent, field)?;
                format!("HashMap<String, {}>", value_ty)
            }
            TypeExpr::Optional(inner) => {
                let inner_ty = self.rust_type(inner, parent, field)?;
                format!("Option<{}>", inner_ty)
            }
            TypeExpr::Nullable(inner) => {
                let inner_ty = self.rust_type(inner, parent, field)?;
                format!("Option<{}>", inner_ty)
            }
            TypeExpr::Object(fields) => {
                let inline_name = inline_type_name(parent, field.unwrap_or("Object"));
                self.emit_struct(&inline_name, fields)?;
                inline_name
            }
            TypeExpr::Union(members) => {
                let inline_name = inline_type_name(parent, field.unwrap_or("Union"));
                self.emit_union(&inline_name, members)?;
                inline_name
            }
        })
    }

    fn rust_type_name(&self, expr: &TypeExpr, parent: &str, field: Option<&str>) -> Result<String> {
        Ok(match expr {
            TypeExpr::Named(name) => self.rust_name(name)?,
            TypeExpr::String => "String".to_string(),
            TypeExpr::Number => "f64".to_string(),
            TypeExpr::Int64 => "i64".to_string(),
            TypeExpr::Float64 => "f64".to_string(),
            TypeExpr::Boolean => "bool".to_string(),
            TypeExpr::Any => "JsonValue".to_string(),
            TypeExpr::Null => "()".to_string(),
            TypeExpr::LiteralString(_) => "String".to_string(),
            TypeExpr::LiteralNumber(_) => "f64".to_string(),
            TypeExpr::Id(_) => "String".to_string(),
            TypeExpr::Array(inner) => {
                let inner_ty = self.rust_type_name(inner, parent, field)?;
                format!("Vec<{}>", inner_ty)
            }
            TypeExpr::Record(key, value) => {
                let key_ty = self.rust_type_name(key, parent, field)?;
                if key_ty != "String" {
                    return Err(anyhow!("Rust only supports String keys for record"));
                }
                let value_ty = self.rust_type_name(value, parent, field)?;
                format!("HashMap<String, {}>", value_ty)
            }
            TypeExpr::Optional(inner) => {
                let inner_ty = self.rust_type_name(inner, parent, field)?;
                format!("Option<{}>", inner_ty)
            }
            TypeExpr::Nullable(inner) => {
                let inner_ty = self.rust_type_name(inner, parent, field)?;
                format!("Option<{}>", inner_ty)
            }
            TypeExpr::Object(_) => inline_type_name(parent, field.unwrap_or("Object")),
            TypeExpr::Union(_) => inline_type_name(parent, field.unwrap_or("Union")),
        })
    }

    fn rust_name(&self, name: &str) -> Result<String> {
        self.name_map
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("No rust name for {}", name))
    }

    fn resolve_named_expr<'a>(&'a self, expr: &'a TypeExpr) -> &'a TypeExpr {
        match expr {
            TypeExpr::Named(name) => self
                .types
                .get(name)
                .map(|ty| self.resolve_named_expr(ty))
                .unwrap_or(expr),
            _ => expr,
        }
    }

    fn field_optional_inner(&self, field: &Field) -> (bool, bool, TypeExpr) {
        let mut is_optional = field.optional;
        let mut is_nullable = field.nullable;
        let mut inner = field.ty.clone();

        loop {
            match inner {
                TypeExpr::Optional(inner_expr) => {
                    is_optional = true;
                    inner = *inner_expr;
                    continue;
                }
                TypeExpr::Nullable(inner_expr) => {
                    is_nullable = true;
                    inner = *inner_expr;
                    continue;
                }
                TypeExpr::Named(ref name) => {
                    if let Some(resolved) = self.types.get(name).cloned() {
                        if matches!(resolved, TypeExpr::Optional(_) | TypeExpr::Nullable(_)) {
                            inner = resolved;
                            continue;
                        }
                    }
                }
                _ => {}
            }
            break;
        }

        (is_optional, is_nullable, inner)
    }

    fn discriminated_union_info(&self, members: &[TypeExpr]) -> Option<(String, Vec<VariantInfo>)> {
        let mut objects = Vec::new();
        for member in members {
            let resolved = self.resolve_named_expr(member);
            match resolved {
                TypeExpr::Object(fields) => objects.push((None, fields.clone())),
                TypeExpr::Named(name) => {
                    if let Some(TypeExpr::Object(fields)) = self.types.get(name) {
                        objects.push((Some(pascal_case(name)), fields.clone()));
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        }

        let mut discriminator: Option<String> = None;
        for (_, obj) in &objects {
            let keys: Vec<_> = obj
                .iter()
                .filter_map(|(k, v)| match v.ty {
                    TypeExpr::LiteralString(_) => Some(k.clone()),
                    _ => None,
                })
                .collect();
            if keys.is_empty() {
                return None;
            }
            if keys.len() > 1 {
                return None;
            }
            let key = keys[0].clone();
            if let Some(existing) = &discriminator {
                if existing != &key {
                    return None;
                }
            } else {
                discriminator = Some(key);
            }
        }

        let disc_key = discriminator?;
        let mut variants = Vec::new();
        for (type_name, obj) in objects {
            let field = obj.get(&disc_key)?;
            let literal = match &field.ty {
                TypeExpr::LiteralString(value) => value.clone(),
                _ => return None,
            };
            variants.push(VariantInfo {
                literal,
                fields: obj,
                type_name,
            });
        }

        Some((disc_key, variants))
    }

    fn union_variant_label(&self, parent: &str, member: &TypeExpr, index: usize) -> String {
        let resolved = self.resolve_named_expr(member);
        match resolved {
            TypeExpr::Named(name) => pascal_case(name),
            TypeExpr::String => "String".to_string(),
            TypeExpr::Number => "Number".to_string(),
            TypeExpr::Int64 => "Int64".to_string(),
            TypeExpr::Float64 => "Float64".to_string(),
            TypeExpr::Boolean => "Bool".to_string(),
            TypeExpr::Any => "Any".to_string(),
            TypeExpr::Null => "Null".to_string(),
            TypeExpr::LiteralString(_) | TypeExpr::LiteralNumber(_) => "Literal".to_string(),
            TypeExpr::Id(_) => "Id".to_string(),
            TypeExpr::Array(inner) => {
                let inner_resolved = self.resolve_named_expr(inner);
                if let TypeExpr::Union(members) = inner_resolved {
                    if self.discriminated_union_info(members).is_some() {
                        return "Parts".to_string();
                    }
                }
                match inner_resolved {
                    TypeExpr::Named(name) => format!("{}List", pascal_case(name)),
                    _ => "Array".to_string(),
                }
            }
            TypeExpr::Object(_) => format!("{}Object{}", parent, index),
            TypeExpr::Union(_) => format!("{}Union{}", parent, index),
            TypeExpr::Optional(inner) => {
                let inner_label = self.union_variant_label(parent, inner, index);
                format!("{}Optional", inner_label)
            }
            TypeExpr::Nullable(inner) => {
                let inner_label = self.union_variant_label(parent, inner, index);
                format!("{}Nullable", inner_label)
            }
            TypeExpr::Record(_, _) => "Record".to_string(),
        }
    }
}
