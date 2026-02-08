pub(crate) fn table_type_name(table_name: &str) -> String {
    let singular = if table_name.ends_with('s') {
        &table_name[..table_name.len() - 1]
    } else {
        table_name
    };
    pascal_case(singular)
}

pub(crate) fn pascal_case(name: &str) -> String {
    let words = split_words(name);
    words
        .into_iter()
        .map(|w| capitalize(&w))
        .collect::<Vec<_>>()
        .join("")
}

pub(crate) fn function_args_type_name(module: &str, name: &str) -> String {
    pascal_case(&format!("{}_{}_args", module, name))
}

pub(crate) fn function_return_type_name(module: &str, name: &str) -> String {
    pascal_case(&format!("{}_{}_return", module, name))
}

pub(crate) fn lower_camel(name: &str) -> String {
    let words = split_words(name);
    let mut iter = words.into_iter();
    let first = iter.next().unwrap_or_default();
    let mut result = first.to_lowercase();
    for w in iter {
        result.push_str(&capitalize(&w));
    }
    result
}

fn split_words(name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();

    let push_current = |out: &mut Vec<String>, current: &mut String| {
        if !current.is_empty() {
            out.push(current.clone());
            current.clear();
        }
    };

    let mut chars = name.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '_' || ch == '-' || ch == '/' || ch == '.' || ch == ':' {
            push_current(&mut out, &mut current);
            continue;
        }

        if ch.is_uppercase() {
            if !current.is_empty() {
                push_current(&mut out, &mut current);
            }
            current.push(ch.to_ascii_lowercase());
        } else if ch.is_numeric() {
            if !current.is_empty() {
                push_current(&mut out, &mut current);
            }
            current.push(ch);
        } else {
            current.push(ch);
        }

        if let Some(next) = chars.peek() {
            if next.is_uppercase() {
                push_current(&mut out, &mut current);
            }
        }
    }

    push_current(&mut out, &mut current);

    out.into_iter().map(normalize_acronyms).collect()
}

fn normalize_acronyms(word: String) -> String {
    match word.as_str() {
        "srs" => "SRS".to_string(),
        "ai" => "AI".to_string(),
        "ui" => "UI".to_string(),
        "id" => "ID".to_string(),
        "l1" => "L1".to_string(),
        "l2" => "L2".to_string(),
        "json" => "JSON".to_string(),
        "llm" => "LLM".to_string(),
        _ => word,
    }
}

fn capitalize(word: &str) -> String {
    if word.is_empty() {
        return String::new();
    }
    if word.chars().all(|c| c.is_uppercase()) {
        return word.to_string();
    }
    let mut chars = word.chars();
    let first = chars
        .next()
        .expect("checked for empty")
        .to_uppercase()
        .to_string();
    let rest = chars.as_str().to_lowercase();
    format!("{}{}", first, rest)
}

pub(crate) fn inline_type_name(parent: &str, field: &str) -> String {
    let mut name = String::new();
    name.push_str(parent);
    name.push_str(&pascal_case(field));
    name
}

pub(crate) fn swift_field_name(field: &str) -> String {
    if is_swift_keyword(field) {
        format!("`{}`", field)
    } else {
        field.to_string()
    }
}

pub(crate) fn swift_member_name(field: &str) -> (String, Option<String>) {
    if field.starts_with('_') {
        return (swift_field_name(field), None);
    }

    if !field.contains('_') {
        return (swift_field_name(field), None);
    }

    let mut out = String::new();
    for (idx, part) in field.split('_').enumerate() {
        if part.is_empty() {
            continue;
        }
        if idx == 0 {
            out.push_str(part);
        } else {
            let mut chars = part.chars();
            if let Some(first) = chars.next() {
                out.push(first.to_ascii_uppercase());
                out.push_str(chars.as_str());
            }
        }
    }

    if out.is_empty() {
        out = field.to_string();
    }

    let swift_name = swift_field_name(&out);
    if swift_name == field {
        (swift_name, None)
    } else {
        (swift_name, Some(field.to_string()))
    }
}

pub(crate) fn swift_case_name(literal: &str) -> String {
    let mut name = lower_camel(literal);
    if name.is_empty() {
        name = "case".to_string();
    }
    if name
        .chars()
        .next()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(false)
    {
        name = format!("case{}", name);
    }
    if is_swift_keyword(&name) {
        format!("`{}`", name)
    } else {
        name
    }
}

pub(crate) fn literal_literal(literal: &str, is_number: bool) -> String {
    if is_number {
        literal.to_string()
    } else {
        format!("\"{}\"", literal)
    }
}

pub(crate) fn is_swift_keyword(word: &str) -> bool {
    word == "internal"
}

pub(crate) fn rust_field_name(field: &str) -> (String, Option<String>) {
    let mut out = String::new();
    let chars: Vec<char> = field.chars().collect();
    for (idx, ch) in chars.iter().enumerate() {
        if ch.is_ascii_uppercase() {
            let prev = idx.checked_sub(1).and_then(|i| chars.get(i));
            let next = chars.get(idx + 1);
            let needs_underscore = match (prev, next) {
                (Some(prev), Some(next)) => {
                    (prev.is_ascii_lowercase() || prev.is_ascii_digit())
                        || (prev.is_ascii_uppercase() && next.is_ascii_lowercase())
                }
                (Some(prev), None) => prev.is_ascii_lowercase() || prev.is_ascii_digit(),
                (None, _) => false,
            };
            if needs_underscore {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else if ch.is_ascii_alphanumeric() || *ch == '_' {
            if out.is_empty() && ch.is_ascii_digit() {
                out.push('_');
            }
            out.push(*ch);
        } else {
            out.push('_');
        }
    }

    if out.is_empty() {
        out = "_field".to_string();
    }

    let mut rust_name = out;
    let mut rename = None;
    if is_rust_keyword(&rust_name) {
        rename = Some(field.to_string());
        rust_name = format!("r#{}", rust_name);
    } else if rust_name != field {
        rename = Some(field.to_string());
    }

    (rust_name, rename)
}

pub(crate) fn rust_variant_name(literal: &str) -> String {
    let mut name = pascal_case(literal);
    if name.is_empty() {
        name = "Variant".to_string();
    }
    if name
        .chars()
        .next()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(false)
    {
        name = format!("V{}", name);
    }
    if is_rust_keyword(&name.to_lowercase()) {
        format!("{}Variant", name)
    } else {
        name
    }
}

pub(crate) fn rust_fn_name(field: &str) -> String {
    let mut out = String::new();
    let mut upper = false;
    for ch in field.chars() {
        if ch == '-' || ch == ':' || ch == '/' || ch == '.' || ch == '_' {
            upper = false;
            out.push('_');
            continue;
        }
        if out.is_empty() {
            out.push(ch.to_ascii_lowercase());
            continue;
        }
        if upper {
            out.push(ch.to_ascii_uppercase());
            upper = false;
            continue;
        }
        if ch.is_uppercase() {
            out.push('_');
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch.to_ascii_lowercase());
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    if is_rust_keyword(&out) {
        format!("r#{}", out)
    } else {
        out
    }
}

fn is_rust_keyword(word: &str) -> bool {
    matches!(
        word,
        "as" | "break"
            | "const"
            | "continue"
            | "crate"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "fn"
            | "for"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "Self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "type"
            | "unsafe"
            | "use"
            | "where"
            | "while"
            | "async"
            | "await"
            | "dyn"
    )
}
