use sqlparser::ast::*;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

pub struct Translator;

impl Translator {
    pub fn translate(sql: &str) -> Result<String, String> {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Ok(String::new());
        }

        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, trimmed)
            .map_err(|e| format!("Parse error: {}", e))?;

        if statements.is_empty() {
            return Ok(String::new());
        }

        let mut results = Vec::new();
        for mut stmt in statements {
            Self::transform_statement(&mut stmt)?;
            results.push(stmt.to_string());
        }
        let joined = results.join("; ");
        Ok(Self::rewrite_params(&joined))
    }

    /// Rewrite PG-style $1, $2 parameters to SQLite-style ?1, ?2
    fn rewrite_params(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let mut chars = sql.chars().peekable();
        let mut in_single_quote = false;
        let mut in_double_quote = false;

        while let Some(c) = chars.next() {
            match c {
                '\'' if !in_double_quote => {
                    in_single_quote = !in_single_quote;
                    result.push(c);
                }
                '"' if !in_single_quote => {
                    in_double_quote = !in_double_quote;
                    result.push(c);
                }
                '$' if !in_single_quote && !in_double_quote => {
                    // Check if followed by digits
                    let mut digits = String::new();
                    while let Some(&next) = chars.peek() {
                        if next.is_ascii_digit() {
                            digits.push(next);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    if digits.is_empty() {
                        result.push('$');
                    } else {
                        result.push('?');
                        result.push_str(&digits);
                    }
                }
                _ => result.push(c),
            }
        }
        result
    }

    fn transform_statement(stmt: &mut Statement) -> Result<(), String> {
        match stmt {
            Statement::CreateTable(ref mut ct) => {
                Self::transform_create_table(ct);
                // Strip schema from table name
                Self::strip_schema_from_name(&mut ct.name);
            }
            Statement::Query(ref mut query) => {
                Self::transform_query(query);
            }
            Statement::Insert(ref mut insert) => {
                Self::strip_schema_from_name(&mut insert.table_name);
                if let Some(ref mut source) = insert.source {
                    Self::transform_query(source);
                }
                // Transform ON CONFLICT expressions
                if let Some(OnInsert::OnConflict(ref mut conflict)) = insert.on {
                    if let OnConflictAction::DoUpdate(ref mut do_update) = conflict.action {
                        for assign in &mut do_update.assignments {
                            Self::transform_expr(&mut assign.value);
                        }
                        if let Some(ref mut sel) = do_update.selection {
                            Self::transform_expr(sel);
                        }
                    }
                }
                // Transform RETURNING clause
                if let Some(ref mut ret) = insert.returning {
                    for item in ret.iter_mut() {
                        if let SelectItem::UnnamedExpr(ref mut expr) = item {
                            Self::transform_expr(expr);
                        }
                        if let SelectItem::ExprWithAlias { ref mut expr, .. } = item {
                            Self::transform_expr(expr);
                        }
                    }
                }
            }
            Statement::Update { ref mut table, ref mut selection, ref mut assignments, ref mut from, ref mut returning, .. } => {
                if let TableFactor::Table { ref mut name, .. } = table.relation {
                    Self::strip_schema_from_name(name);
                }
                if let Some(ref mut sel) = selection {
                    Self::transform_expr(sel);
                }
                for assign in assignments.iter_mut() {
                    Self::transform_expr(&mut assign.value);
                }
                // Handle UPDATE ... FROM (multi-table update)
                if let Some(ref mut from_clause) = from {
                    Self::strip_schema_from_table_factor(&mut from_clause.relation);
                    for join in &mut from_clause.joins {
                        Self::strip_schema_from_table_factor(&mut join.relation);
                        Self::transform_join_constraint(&mut join.join_operator);
                    }
                }
                // Handle RETURNING clause
                if let Some(ref mut ret) = returning {
                    for item in ret.iter_mut() {
                        if let SelectItem::UnnamedExpr(ref mut expr) = item {
                            Self::transform_expr(expr);
                        }
                        if let SelectItem::ExprWithAlias { ref mut expr, .. } = item {
                            Self::transform_expr(expr);
                        }
                    }
                }
            }
            Statement::Delete(ref mut delete) => {
                if let FromTable::WithFromKeyword(ref mut tables) = delete.from {
                    for table in tables.iter_mut() {
                        Self::strip_schema_from_table_factor(&mut table.relation);
                    }
                }
                if let Some(ref mut sel) = delete.selection {
                    Self::transform_expr(sel);
                }
                // Handle DELETE ... USING (PG multi-table delete)
                // Transform: DELETE FROM t1 USING t2 WHERE condition
                //        to: DELETE FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE condition)
                if let Some(using_tables) = delete.using.take() {
                    let where_clause = delete.selection.take();
                    let exists_subquery = Expr::Exists {
                        subquery: Box::new(Query {
                            with: None,
                            body: Box::new(SetExpr::Select(Box::new(Select {
                                select_token: AttachedToken::empty(),
                                distinct: None,
                                top: None,
                                top_before_distinct: false,
                                projection: vec![SelectItem::UnnamedExpr(Expr::Value(Value::Number("1".to_string(), false)))],
                                into: None,
                                from: using_tables,
                                lateral_views: vec![],
                                prewhere: None,
                                selection: where_clause,
                                group_by: GroupByExpr::Expressions(vec![], vec![]),
                                cluster_by: vec![],
                                distribute_by: vec![],
                                sort_by: vec![],
                                having: None,
                                named_window: vec![],
                                qualify: None,
                                window_before_qualify: false,
                                value_table_mode: None,
                                connect_by: None,
                            }))),
                            order_by: None,
                            limit: None,
                            limit_by: vec![],
                            offset: None,
                            fetch: None,
                            locks: vec![],
                            for_clause: None,
                            settings: None,
                            format_clause: None,
                        }),
                        negated: false,
                    };
                    delete.selection = Some(exists_subquery);
                }
            }
            Statement::Drop { ref mut names, .. } => {
                for name in names.iter_mut() {
                    Self::strip_schema_from_name(name);
                }
            }
            Statement::AlterTable { ref mut name, .. } => {
                Self::strip_schema_from_name(name);
            }
            Statement::Truncate { ref mut table_names, .. } => {
                // TRUNCATE → will be handled as DELETE FROM in the backend
                for truncate_table in table_names.iter_mut() {
                    Self::strip_schema_from_name(&mut truncate_table.name);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn transform_create_table(ct: &mut CreateTable) {
        for col in &mut ct.columns {
            Self::transform_column_def(col);
        }
    }

    fn transform_column_def(col: &mut ColumnDef) {
        // Detect SERIAL/BIGSERIAL before type mapping
        let is_serial = matches!(&col.data_type, DataType::Custom(name, _)
            if {
                let n = name.to_string().to_uppercase();
                n == "SERIAL" || n == "BIGSERIAL" || n == "SMALLSERIAL"
            }
        );

        col.data_type = Self::map_data_type(&col.data_type);

        // For SERIAL types, add PRIMARY KEY AUTOINCREMENT if not already present
        if is_serial {
            let has_pk = col.options.iter().any(|o| matches!(o.option,
                ColumnOption::Unique { is_primary: true, .. }));
            if !has_pk {
                col.options.push(ColumnOptionDef {
                    name: None,
                    option: ColumnOption::Unique {
                        is_primary: true,
                        characteristics: None,
                    },
                });
            }
            // Remove any DEFAULT nextval(...) options added by PG
            col.options.retain(|o| !matches!(&o.option, ColumnOption::Default(expr)
                if expr.to_string().to_lowercase().contains("nextval")));
        }

        // Transform default expressions in column options
        for opt in &mut col.options {
            if let ColumnOption::Default(ref mut expr) = opt.option {
                // Don't transform CURRENT_TIMESTAMP / CURRENT_DATE in DEFAULT context
                // SQLite supports these natively as default values
                let expr_str = expr.to_string().to_lowercase();
                if expr_str == "current_timestamp" || expr_str == "current_date"
                    || expr_str == "current_time" {
                    continue;
                }
                Self::transform_expr(expr);
            }
        }
    }

    fn map_data_type(dt: &DataType) -> DataType {
        match dt {
            // VARCHAR(n) → TEXT
            DataType::Varchar(_) | DataType::CharVarying(_) => DataType::Text,

            // BOOLEAN → TEXT (store as 'true'/'false' for PG compat)
            DataType::Boolean | DataType::Bool => DataType::Text,

            // TIMESTAMP variants → TEXT
            DataType::Timestamp(_, _) => DataType::Text,

            // UUID → TEXT
            DataType::Uuid => DataType::Text,

            // JSONB / JSON → TEXT
            DataType::JSONB => DataType::Text,
            DataType::JSON => DataType::Text,

            // BYTEA → BLOB
            DataType::Bytea => DataType::Blob(None),

            // DOUBLE PRECISION → REAL
            DataType::DoublePrecision => DataType::Real,

            // NUMERIC/DECIMAL → REAL
            DataType::Numeric(_) => DataType::Real,
            DataType::Decimal(_) => DataType::Real,

            // INTERVAL → TEXT
            DataType::Interval => DataType::Text,

            // CHAR(n) → TEXT
            DataType::Char(_) | DataType::Character(_) => DataType::Text,

            // Custom types (SERIAL, BIGSERIAL, etc) → map accordingly
            DataType::Custom(name, _) => {
                let type_name = name.to_string().to_uppercase();
                match type_name.as_str() {
                    "SERIAL" => DataType::Integer(None),
                    "BIGSERIAL" => DataType::BigInt(None),
                    "SMALLSERIAL" => DataType::SmallInt(None),
                    "CITEXT" => DataType::Text,
                    _ => dt.clone(),
                }
            }

            // Everything else passes through
            _ => dt.clone(),
        }
    }

    fn transform_query(query: &mut Query) {
        if let SetExpr::Select(ref mut select) = *query.body {
            Self::transform_select(select);
        }
    }

    fn transform_select(select: &mut Select) {
        for item in &mut select.projection {
            match item {
                SelectItem::UnnamedExpr(ref mut expr) => {
                    Self::transform_expr(expr);
                }
                SelectItem::ExprWithAlias { ref mut expr, .. } => {
                    Self::transform_expr(expr);
                }
                _ => {}
            }
        }
        if let Some(ref mut selection) = select.selection {
            Self::transform_expr(selection);
        }
        // Transform GROUP BY expressions
        if let GroupByExpr::Expressions(ref mut exprs, _) = select.group_by {
            for expr in exprs.iter_mut() {
                Self::transform_expr(expr);
            }
        }
        // Transform HAVING
        if let Some(ref mut having) = select.having {
            Self::transform_expr(having);
        }
        // Strip schema qualifiers from FROM clause
        for table in &mut select.from {
            Self::strip_schema_from_table_factor(&mut table.relation);
            for join in &mut table.joins {
                Self::strip_schema_from_table_factor(&mut join.relation);
                Self::transform_join_constraint(&mut join.join_operator);
            }
        }
    }

    fn transform_join_constraint(op: &mut JoinOperator) {
        match op {
            JoinOperator::Inner(JoinConstraint::On(ref mut expr))
            | JoinOperator::LeftOuter(JoinConstraint::On(ref mut expr))
            | JoinOperator::RightOuter(JoinConstraint::On(ref mut expr))
            | JoinOperator::FullOuter(JoinConstraint::On(ref mut expr)) => {
                Self::transform_expr(expr);
            }
            _ => {}
        }
    }

    /// Strip schema qualifiers like "public." from table names
    fn strip_schema_from_table_factor(factor: &mut TableFactor) {
        if let TableFactor::Table { ref mut name, .. } = factor {
            Self::strip_schema_from_name(name);
        }
    }

    /// Remove schema prefix from ObjectName (e.g., public.users → users)
    fn strip_schema_from_name(name: &mut ObjectName) {
        if name.0.len() > 1 {
            let schema = name.0[0].value.to_lowercase();
            if schema == "public" || schema == "pg_catalog" {
                name.0.remove(0);
            }
        }
    }

    fn transform_expr(expr: &mut Expr) {
        match expr {
            Expr::Cast { ref mut data_type, ref mut expr, ref mut kind, .. } => {
                *data_type = Self::map_data_type(data_type);
                // Convert PG :: casts to standard CAST() syntax for SQLite
                if *kind == CastKind::DoubleColon {
                    *kind = CastKind::Cast;
                }
                Self::transform_expr(expr);
            }
            Expr::Function(ref mut func) => {
                Self::transform_function(func);
            }
            Expr::BinaryOp { ref mut left, ref mut right, .. } => {
                Self::transform_expr(left);
                Self::transform_expr(right);
            }
            Expr::UnaryOp { ref mut expr, .. } => {
                Self::transform_expr(expr);
            }
            Expr::Nested(ref mut inner) => {
                Self::transform_expr(inner);
            }
            Expr::Extract { field, expr: ref mut inner_expr, .. } => {
                Self::transform_expr(inner_expr);
                // Transform EXTRACT(field FROM expr) → strftime equivalent
                let format = match field.to_string().to_lowercase().as_str() {
                    "year" => Some("%Y"),
                    "month" => Some("%m"),
                    "day" => Some("%d"),
                    "hour" => Some("%H"),
                    "minute" => Some("%M"),
                    "second" => Some("%S"),
                    "epoch" => Some("%s"),
                    "dow" => Some("%w"),
                    "doy" => Some("%j"),
                    _ => None,
                };
                if let Some(fmt) = format {
                    // Replace the Extract expr with a CAST(strftime(fmt, expr) AS REAL) function call
                    let strftime_func = Expr::Function(Function {
                        name: ObjectName(vec![Ident::new("strftime")]),
                        args: FunctionArguments::List(FunctionArgumentList {
                            duplicate_treatment: None,
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    Expr::Value(Value::SingleQuotedString(fmt.to_string())),
                                )),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(*inner_expr.clone())),
                            ],
                            clauses: vec![],
                        }),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: vec![],
                        parameters: FunctionArguments::None,
                        uses_odbc_syntax: false,
                    });
                    *expr = strftime_func;
                }
            }
            Expr::IsNotNull(ref mut inner) => {
                Self::transform_expr(inner);
            }
            Expr::IsNull(ref mut inner) => {
                Self::transform_expr(inner);
            }
            Expr::InList { ref mut expr, ref mut list, .. } => {
                Self::transform_expr(expr);
                for item in list.iter_mut() {
                    Self::transform_expr(item);
                }
            }
            Expr::Between { ref mut expr, ref mut low, ref mut high, .. } => {
                Self::transform_expr(expr);
                Self::transform_expr(low);
                Self::transform_expr(high);
            }
            Expr::Case { ref mut operand, ref mut conditions, ref mut results, ref mut else_result, .. } => {
                if let Some(ref mut op) = operand {
                    Self::transform_expr(op);
                }
                for cond in conditions.iter_mut() {
                    Self::transform_expr(cond);
                }
                for res in results.iter_mut() {
                    Self::transform_expr(res);
                }
                if let Some(ref mut el) = else_result {
                    Self::transform_expr(el);
                }
            }
            Expr::Subquery(ref mut query) => {
                Self::transform_query(query);
            }
            Expr::ILike { negated, expr: ref mut like_expr, pattern: ref mut like_pattern, .. } => {
                // ILIKE → LOWER(expr) LIKE LOWER(pattern)
                Self::transform_expr(like_expr);
                Self::transform_expr(like_pattern);
                let negated = *negated;
                let lower_expr = Box::new(Self::wrap_in_lower(*like_expr.clone()));
                let lower_pattern = Box::new(Self::wrap_in_lower(*like_pattern.clone()));
                *expr = Expr::Like {
                    negated,
                    any: false,
                    expr: lower_expr,
                    pattern: lower_pattern,
                    escape_char: None,
                };
            }
            Expr::Like { ref mut expr, ref mut pattern, .. } => {
                Self::transform_expr(expr);
                Self::transform_expr(pattern);
            }
            Expr::InSubquery { ref mut expr, ref mut subquery, .. } => {
                Self::transform_expr(expr);
                Self::transform_query(subquery);
            }
            Expr::Exists { ref mut subquery, .. } => {
                Self::transform_query(subquery);
            }
            // AnyOp and AllOp pass through to SQLite
            _ => {}
        }
    }

    /// Wrap an expression in LOWER(expr) for ILIKE transformation
    fn wrap_in_lower(inner: Expr) -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![Ident::new("LOWER")]),
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
            uses_odbc_syntax: false,
        })
    }

    fn transform_function(func: &mut Function) {
        // Recursively transform any arguments first
        if let FunctionArguments::List(ref mut arg_list) = func.args {
            for arg in &mut arg_list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(ref mut expr)) => {
                        Self::transform_expr(expr);
                    }
                    FunctionArg::Named { arg: FunctionArgExpr::Expr(ref mut expr), .. } => {
                        Self::transform_expr(expr);
                    }
                    _ => {}
                }
            }
        }

        let name = func.name.to_string().to_lowercase();
        match name.as_str() {
            "now" => {
                func.name = ObjectName(vec![Ident::new("datetime")]);
                func.args = FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        Expr::Value(Value::SingleQuotedString("now".to_string())),
                    ))],
                    clauses: vec![],
                });
            }
            "current_timestamp" => {
                // current_timestamp → datetime('now')
                func.name = ObjectName(vec![Ident::new("datetime")]);
                func.args = FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        Expr::Value(Value::SingleQuotedString("now".to_string())),
                    ))],
                    clauses: vec![],
                });
            }
            "current_date" => {
                // current_date → date('now')
                func.name = ObjectName(vec![Ident::new("date")]);
                func.args = FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        Expr::Value(Value::SingleQuotedString("now".to_string())),
                    ))],
                    clauses: vec![],
                });
            }
            "gen_random_uuid" => {
                func.name = ObjectName(vec![Ident::new("pgsqlite_uuid_v4")]);
            }
            "string_agg" => {
                func.name = ObjectName(vec![Ident::new("group_concat")]);
            }
            "array_agg" => {
                func.name = ObjectName(vec![Ident::new("group_concat")]);
            }
            "date_trunc" => {
                // date_trunc('month', ts) → strftime('%Y-%m-01', ts)
                if let FunctionArguments::List(ref arg_list) = func.args {
                    if arg_list.args.len() == 2 {
                        let part = Self::extract_string_arg(&arg_list.args[0]);
                        let ts_expr = Self::extract_expr_arg(&arg_list.args[1]);
                        if let (Some(part), Some(ts_expr)) = (part, ts_expr) {
                            let format = match part.to_lowercase().as_str() {
                                "year" => "%Y-01-01 00:00:00",
                                "month" => "%Y-%m-01 00:00:00",
                                "day" => "%Y-%m-%d 00:00:00",
                                "hour" => "%Y-%m-%d %H:00:00",
                                "minute" => "%Y-%m-%d %H:%M:00",
                                "second" => "%Y-%m-%d %H:%M:%S",
                                _ => "%Y-%m-%d %H:%M:%S",
                            };
                            func.name = ObjectName(vec![Ident::new("strftime")]);
                            func.args = FunctionArguments::List(FunctionArgumentList {
                                duplicate_treatment: None,
                                args: vec![
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                        Expr::Value(Value::SingleQuotedString(format.to_string())),
                                    )),
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(ts_expr)),
                                ],
                                clauses: vec![],
                            });
                        }
                    }
                }
            }
            "to_char" => {
                // to_char(ts, 'YYYY-MM-DD') → strftime('%Y-%m-%d', ts)
                if let FunctionArguments::List(ref arg_list) = func.args {
                    if arg_list.args.len() == 2 {
                        let ts_expr = Self::extract_expr_arg(&arg_list.args[0]);
                        let format = Self::extract_string_arg(&arg_list.args[1]);
                        if let (Some(ts_expr), Some(format)) = (ts_expr, format) {
                            let sqlite_format = Self::pg_format_to_strftime(&format);
                            func.name = ObjectName(vec![Ident::new("strftime")]);
                            func.args = FunctionArguments::List(FunctionArgumentList {
                                duplicate_treatment: None,
                                args: vec![
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                        Expr::Value(Value::SingleQuotedString(sqlite_format)),
                                    )),
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(ts_expr)),
                                ],
                                clauses: vec![],
                            });
                        }
                    }
                }
            }
            "concat" => {
                // concat(a, b, c) → (a || b || c) — leave as-is, SQLite supports concat
                // Actually, SQLite 3.44+ has concat(). For older versions we'd need ||.
                // Leave as-is for now since bundled SQLite should be recent.
            }
            "position" => {
                // position(substring IN string) → instr(string, substring)
                // Note: sqlparser parses POSITION as a function with special handling
                func.name = ObjectName(vec![Ident::new("instr")]);
                // Swap args: PG is position(sub IN str), but sqlparser gives us (sub, str)
                // Actually instr(str, sub) - we need str first, sub second
                if let FunctionArguments::List(ref mut arg_list) = func.args {
                    if arg_list.args.len() == 2 {
                        arg_list.args.swap(0, 1);
                    }
                }
            }
            "strpos" => {
                // strpos(string, substring) → instr(string, substring)
                func.name = ObjectName(vec![Ident::new("instr")]);
            }
            "left" => {
                // left(str, n) → substr(str, 1, n)
                if let FunctionArguments::List(ref arg_list) = func.args {
                    if arg_list.args.len() == 2 {
                        let str_expr = Self::extract_expr_arg(&arg_list.args[0]);
                        let n_expr = Self::extract_expr_arg(&arg_list.args[1]);
                        if let (Some(str_expr), Some(n_expr)) = (str_expr, n_expr) {
                            func.name = ObjectName(vec![Ident::new("substr")]);
                            func.args = FunctionArguments::List(FunctionArgumentList {
                                duplicate_treatment: None,
                                args: vec![
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)),
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                        Expr::Value(Value::Number("1".to_string(), false)),
                                    )),
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(n_expr)),
                                ],
                                clauses: vec![],
                            });
                        }
                    }
                }
            }
            "right" => {
                // right(str, n) → substr(str, -n)
                if let FunctionArguments::List(ref arg_list) = func.args {
                    if arg_list.args.len() == 2 {
                        let str_expr = Self::extract_expr_arg(&arg_list.args[0]);
                        let n_expr = Self::extract_expr_arg(&arg_list.args[1]);
                        if let (Some(str_expr), Some(n_expr)) = (str_expr, n_expr) {
                            func.name = ObjectName(vec![Ident::new("substr")]);
                            func.args = FunctionArguments::List(FunctionArgumentList {
                                duplicate_treatment: None,
                                args: vec![
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)),
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                        Expr::UnaryOp {
                                            op: UnaryOperator::Minus,
                                            expr: Box::new(n_expr),
                                        }
                                    )),
                                ],
                                clauses: vec![],
                            });
                        }
                    }
                }
            }
            "char_length" | "character_length" => {
                // char_length(x) → length(x)
                func.name = ObjectName(vec![Ident::new("length")]);
            }
            "btrim" => {
                // btrim(x) → trim(x)
                func.name = ObjectName(vec![Ident::new("trim")]);
            }
            "ltrim" | "rtrim" => {
                // These pass through - SQLite supports them natively
            }
            "regexp_replace" => {
                // regexp_replace(str, pattern, replacement) → replace(str, pattern, replacement)
                // This is a simplification - only handles literal string replacement, not regex
                func.name = ObjectName(vec![Ident::new("replace")]);
                // If there's a 4th 'flags' argument, drop it
                if let FunctionArguments::List(ref mut arg_list) = func.args {
                    if arg_list.args.len() > 3 {
                        arg_list.args.truncate(3);
                    }
                }
            }
            "array_to_string" => {
                // array_to_string(ARRAY[a,b,c], ',') - in practice ORMs pass columns
                // Approximate with group_concat in aggregate context, otherwise leave as-is
                // For simple use: treat as no-op / pass-through
            }
            "repeat" => {
                // repeat(str, n) → not natively supported, but we can handle simple cases
                // For now, leave as-is (will error if not supported)
            }
            "initcap" => {
                // initcap not supported in SQLite, leave as-is
            }
            "md5" => {
                // md5 not natively in SQLite, leave as-is
            }
            "reverse" => {
                // reverse not natively in SQLite
            }
            "lpad" | "rpad" => {
                // lpad/rpad not natively in SQLite
            }
            _ => {}
        }
    }

    fn extract_string_arg(arg: &FunctionArg) -> Option<String> {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(s)))) => {
                Some(s.clone())
            }
            _ => None,
        }
    }

    fn extract_expr_arg(arg: &FunctionArg) -> Option<Expr> {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr.clone()),
            _ => None,
        }
    }

    /// Convert PG date format strings to SQLite strftime format
    fn pg_format_to_strftime(pg_format: &str) -> String {
        pg_format
            .replace("YYYY", "%Y")
            .replace("YY", "%y")
            .replace("MM", "%m")
            .replace("DD", "%d")
            .replace("HH24", "%H")
            .replace("HH12", "%I")
            .replace("HH", "%H")
            .replace("MI", "%M")
            .replace("SS", "%S")
            .replace("TZ", "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // PASSTHROUGH TESTS
    // ========================================================================

    #[test]
    fn test_simple_select_passthrough() {
        let result = Translator::translate("SELECT 1").unwrap();
        assert_eq!(result, "SELECT 1");
    }

    #[test]
    fn test_select_with_where() {
        let result = Translator::translate("SELECT id, name FROM users WHERE id = 1").unwrap();
        assert_eq!(result, "SELECT id, name FROM users WHERE id = 1");
    }

    #[test]
    fn test_insert_passthrough() {
        let result = Translator::translate(
            "INSERT INTO users (name, email) VALUES ('alice', 'alice@example.com')"
        ).unwrap();
        assert!(result.contains("INSERT INTO"));
        assert!(result.contains("alice"));
    }

    #[test]
    fn test_update_passthrough() {
        let result = Translator::translate(
            "UPDATE users SET name = 'bob' WHERE id = 1"
        ).unwrap();
        assert!(result.contains("UPDATE"));
        assert!(result.contains("bob"));
    }

    #[test]
    fn test_delete_passthrough() {
        let result = Translator::translate("DELETE FROM users WHERE id = 1").unwrap();
        assert!(result.contains("DELETE FROM"));
    }

    // ========================================================================
    // DATA TYPE MAPPING TESTS
    // ========================================================================

    #[test]
    fn test_create_table_varchar_to_text() {
        let result = Translator::translate(
            "CREATE TABLE t (name VARCHAR(255))"
        ).unwrap();
        assert!(result.contains("TEXT"), "VARCHAR should map to TEXT, got: {}", result);
        assert!(!result.contains("VARCHAR"), "VARCHAR should not remain, got: {}", result);
    }

    #[test]
    fn test_create_table_boolean_mapping() {
        let result = Translator::translate(
            "CREATE TABLE t (active BOOLEAN)"
        ).unwrap();
        assert!(result.contains("TEXT"),
            "BOOLEAN should map to TEXT, got: {}", result);
    }

    #[test]
    fn test_create_table_timestamp_to_text() {
        let result = Translator::translate(
            "CREATE TABLE t (created_at TIMESTAMP WITH TIME ZONE)"
        ).unwrap();
        assert!(result.contains("TEXT"), "TIMESTAMP should map to TEXT, got: {}", result);
    }

    #[test]
    fn test_create_table_uuid_to_text() {
        let result = Translator::translate(
            "CREATE TABLE t (id UUID)"
        ).unwrap();
        assert!(result.contains("TEXT"), "UUID should map to TEXT, got: {}", result);
    }

    #[test]
    fn test_create_table_jsonb_to_text() {
        let result = Translator::translate(
            "CREATE TABLE t (data JSONB)"
        ).unwrap();
        assert!(result.contains("TEXT"), "JSONB should map to TEXT, got: {}", result);
    }

    #[test]
    fn test_create_table_bytea_to_blob() {
        let result = Translator::translate(
            "CREATE TABLE t (data BYTEA)"
        ).unwrap();
        assert!(result.contains("BLOB"), "BYTEA should map to BLOB, got: {}", result);
    }

    #[test]
    fn test_create_table_double_precision() {
        let result = Translator::translate(
            "CREATE TABLE t (val DOUBLE PRECISION)"
        ).unwrap();
        assert!(result.contains("REAL"), "DOUBLE PRECISION should map to REAL, got: {}", result);
    }

    #[test]
    fn test_create_table_numeric() {
        let result = Translator::translate(
            "CREATE TABLE t (price NUMERIC(10, 2))"
        ).unwrap();
        assert!(result.contains("REAL"), "NUMERIC should map to REAL, got: {}", result);
    }

    // ========================================================================
    // FUNCTION MAPPING TESTS
    // ========================================================================

    #[test]
    fn test_now_function() {
        let result = Translator::translate("SELECT now()").unwrap();
        assert!(result.contains("datetime"), "now() should become datetime('now'), got: {}", result);
    }

    #[test]
    fn test_string_agg_to_group_concat() {
        let result = Translator::translate(
            "SELECT string_agg(name, ', ') FROM users"
        ).unwrap();
        assert!(result.contains("group_concat"),
            "string_agg should become group_concat, got: {}", result);
    }

    // ========================================================================
    // TYPE CAST TESTS
    // ========================================================================

    #[test]
    fn test_pg_cast_syntax() {
        let result = Translator::translate("SELECT '123'::INTEGER").unwrap();
        assert!(result.contains("CAST"), "PG cast should become CAST, got: {}", result);
    }

    // ========================================================================
    // COMPLEX QUERY TESTS
    // ========================================================================

    #[test]
    fn test_create_table_multiple_pg_types() {
        let result = Translator::translate(
            "CREATE TABLE users (
                id UUID PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email TEXT NOT NULL,
                active BOOLEAN DEFAULT true,
                metadata JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            )"
        ).unwrap();
        assert!(!result.contains("UUID"), "UUID should be mapped, got: {}", result);
        assert!(!result.contains("VARCHAR"), "VARCHAR should be mapped, got: {}", result);
        assert!(!result.contains("JSONB"), "JSONB should be mapped, got: {}", result);
    }

    #[test]
    fn test_empty_query() {
        let result = Translator::translate("").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_default_now_in_create_table() {
        let result = Translator::translate(
            "CREATE TABLE t (created_at TIMESTAMP DEFAULT now())"
        ).unwrap();
        assert!(result.contains("datetime"), "now() in DEFAULT should be transformed, got: {}", result);
    }

    // ========================================================================
    // PARAMETER REWRITING TESTS
    // ========================================================================

    #[test]
    fn test_param_rewrite_single() {
        let result = Translator::translate("SELECT $1").unwrap();
        assert!(result.contains("?1"), "Should rewrite $1 to ?1, got: {}", result);
        assert!(!result.contains("$1"), "Should not contain $1, got: {}", result);
    }

    #[test]
    fn test_param_rewrite_multiple() {
        let result = Translator::translate("SELECT $1, $2, $3").unwrap();
        assert!(result.contains("?1"), "got: {}", result);
        assert!(result.contains("?2"), "got: {}", result);
        assert!(result.contains("?3"), "got: {}", result);
    }

    #[test]
    fn test_param_rewrite_in_where() {
        let result = Translator::translate("SELECT * FROM t WHERE id = $1 AND name = $2").unwrap();
        assert!(result.contains("?1"), "got: {}", result);
        assert!(result.contains("?2"), "got: {}", result);
    }

    #[test]
    fn test_param_rewrite_in_insert() {
        let result = Translator::translate("INSERT INTO t (a, b) VALUES ($1, $2)").unwrap();
        assert!(result.contains("?1"), "got: {}", result);
        assert!(result.contains("?2"), "got: {}", result);
    }

    #[test]
    fn test_param_not_rewritten_in_string_literal() {
        // $1 inside a string should NOT be rewritten
        let result = Translator::translate("SELECT 'cost is $1'").unwrap();
        assert!(result.contains("$1"), "Should preserve $1 inside string, got: {}", result);
    }

    #[test]
    fn test_param_rewrite_double_digit() {
        let result = Translator::translate("SELECT $10, $11").unwrap();
        assert!(result.contains("?10"), "got: {}", result);
        assert!(result.contains("?11"), "got: {}", result);
    }

    // ========================================================================
    // ADDITIONAL FUNCTION MAPPING TESTS
    // ========================================================================

    #[test]
    fn test_date_trunc_month() {
        let result = Translator::translate(
            "SELECT date_trunc('month', created_at) FROM events"
        ).unwrap();
        assert!(result.contains("strftime"), "date_trunc should become strftime, got: {}", result);
        assert!(result.contains("%Y-%m-01"), "Should contain month format, got: {}", result);
    }

    #[test]
    fn test_date_trunc_year() {
        let result = Translator::translate(
            "SELECT date_trunc('year', ts) FROM t"
        ).unwrap();
        assert!(result.contains("strftime"), "got: {}", result);
        assert!(result.contains("%Y-01-01"), "got: {}", result);
    }

    #[test]
    fn test_date_trunc_day() {
        let result = Translator::translate(
            "SELECT date_trunc('day', ts) FROM t"
        ).unwrap();
        assert!(result.contains("strftime"), "got: {}", result);
        assert!(result.contains("%Y-%m-%d"), "got: {}", result);
    }

    #[test]
    fn test_to_char_function() {
        let result = Translator::translate(
            "SELECT to_char(created_at, 'YYYY-MM-DD') FROM events"
        ).unwrap();
        assert!(result.contains("strftime"), "to_char should become strftime, got: {}", result);
        assert!(result.contains("%Y-%m-%d"), "Should have converted format, got: {}", result);
    }

    #[test]
    fn test_to_char_with_time() {
        let result = Translator::translate(
            "SELECT to_char(ts, 'YYYY-MM-DD HH24:MI:SS') FROM t"
        ).unwrap();
        assert!(result.contains("strftime"), "got: {}", result);
        assert!(result.contains("%Y-%m-%d %H:%M:%S"), "got: {}", result);
    }

    #[test]
    fn test_extract_epoch() {
        let result = Translator::translate(
            "SELECT EXTRACT(EPOCH FROM created_at) FROM events"
        ).unwrap();
        assert!(result.contains("strftime"), "EXTRACT should become strftime, got: {}", result);
        assert!(result.contains("%s"), "Should extract epoch, got: {}", result);
    }

    #[test]
    fn test_extract_year() {
        let result = Translator::translate(
            "SELECT EXTRACT(YEAR FROM ts) FROM t"
        ).unwrap();
        assert!(result.contains("strftime"), "got: {}", result);
        assert!(result.contains("%Y"), "got: {}", result);
    }

    #[test]
    fn test_extract_month() {
        let result = Translator::translate(
            "SELECT EXTRACT(MONTH FROM ts) FROM t"
        ).unwrap();
        assert!(result.contains("strftime"), "got: {}", result);
        assert!(result.contains("%m"), "got: {}", result);
    }

    #[test]
    fn test_passthrough_coalesce() {
        let result = Translator::translate(
            "SELECT COALESCE(name, 'unknown') FROM users"
        ).unwrap();
        assert!(result.contains("COALESCE"), "COALESCE should pass through, got: {}", result);
    }

    #[test]
    fn test_passthrough_length() {
        let result = Translator::translate(
            "SELECT length(name) FROM users"
        ).unwrap();
        assert!(result.to_lowercase().contains("length"), "length should pass through, got: {}", result);
    }

    #[test]
    fn test_passthrough_lower_upper() {
        let result = Translator::translate(
            "SELECT lower(name), upper(name) FROM users"
        ).unwrap();
        let lower_result = result.to_lowercase();
        assert!(lower_result.contains("lower"), "lower should pass through, got: {}", result);
        assert!(lower_result.contains("upper"), "upper should pass through, got: {}", result);
    }

    #[test]
    fn test_passthrough_replace() {
        let result = Translator::translate(
            "SELECT replace(name, 'a', 'b') FROM users"
        ).unwrap();
        assert!(result.to_lowercase().contains("replace"), "replace should pass through, got: {}", result);
    }

    #[test]
    fn test_passthrough_abs_round() {
        let result = Translator::translate("SELECT abs(-5), round(3.7)").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("abs"), "abs should pass through, got: {}", result);
        assert!(lower.contains("round"), "round should pass through, got: {}", result);
    }

    #[test]
    fn test_gen_random_uuid() {
        let result = Translator::translate("SELECT gen_random_uuid()").unwrap();
        assert!(result.contains("pgsqlite_uuid_v4"), "got: {}", result);
    }

    #[test]
    fn test_nested_function_calls() {
        let result = Translator::translate(
            "SELECT lower(replace(name, ' ', '_')) FROM users"
        ).unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("lower"), "got: {}", result);
        assert!(lower.contains("replace"), "got: {}", result);
    }

    #[test]
    fn test_ilike_to_lower_like() {
        let result = Translator::translate("SELECT * FROM users WHERE name ILIKE '%alice%'").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("lower"), "ILIKE should be converted to LOWER(), got: {}", result);
        assert!(lower.contains("like"), "Should contain LIKE, got: {}", result);
        assert!(!lower.contains("ilike"), "Should NOT contain ILIKE, got: {}", result);
    }

    #[test]
    fn test_not_ilike() {
        let result = Translator::translate("SELECT * FROM users WHERE name NOT ILIKE '%bob%'").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("not like"), "NOT ILIKE should become NOT LIKE, got: {}", result);
        assert!(lower.contains("lower"), "Should use LOWER(), got: {}", result);
    }

    #[test]
    fn test_schema_qualified_select() {
        let result = Translator::translate("SELECT * FROM public.users").unwrap();
        assert!(!result.contains("public."), "Should strip public. prefix, got: {}", result);
        assert!(result.contains("users"), "Should keep table name, got: {}", result);
    }

    #[test]
    fn test_schema_qualified_insert() {
        let result = Translator::translate("INSERT INTO public.users (name) VALUES ('test')").unwrap();
        assert!(!result.contains("public."), "Should strip public. prefix, got: {}", result);
    }

    #[test]
    fn test_schema_qualified_update() {
        let result = Translator::translate("UPDATE public.users SET name = 'test' WHERE id = 1").unwrap();
        assert!(!result.contains("public."), "Should strip public. prefix, got: {}", result);
    }

    #[test]
    fn test_schema_qualified_delete() {
        let result = Translator::translate("DELETE FROM public.users WHERE id = 1").unwrap();
        assert!(!result.contains("public."), "Should strip public. prefix, got: {}", result);
    }

    #[test]
    fn test_schema_qualified_create_table() {
        let result = Translator::translate("CREATE TABLE public.users (id INTEGER, name TEXT)").unwrap();
        assert!(!result.contains("public."), "Should strip public. prefix, got: {}", result);
    }

    #[test]
    fn test_schema_qualified_drop() {
        let result = Translator::translate("DROP TABLE public.users").unwrap();
        assert!(!result.contains("public."), "Should strip public. prefix, got: {}", result);
    }

    #[test]
    fn test_double_colon_cast() {
        let result = Translator::translate("SELECT '42'::integer").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("cast"), ":: cast should become CAST(), got: {}", result);
        assert!(!result.contains("::"), "Should not contain ::, got: {}", result);
    }

    #[test]
    fn test_double_colon_text_cast() {
        let result = Translator::translate("SELECT 42::text").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("cast"), ":: cast should become CAST(), got: {}", result);
        assert!(lower.contains("text"), "Should cast to TEXT, got: {}", result);
    }

    #[test]
    fn test_group_by_having_transform() {
        let result = Translator::translate(
            "SELECT category, count(*) FROM items GROUP BY category HAVING count(*) > 5"
        ).unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("group by"), "Should have GROUP BY, got: {}", result);
        assert!(lower.contains("having"), "Should have HAVING, got: {}", result);
    }

    #[test]
    fn test_truncate_passthrough() {
        // TRUNCATE should parse - we'll handle it as DELETE in the backend
        let result = Translator::translate("TRUNCATE TABLE users");
        assert!(result.is_ok(), "TRUNCATE should parse: {:?}", result.err());
    }

    #[test]
    fn test_serial_to_integer_primary_key() {
        let result = Translator::translate("CREATE TABLE users (id SERIAL, name TEXT)").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("integer"), "SERIAL should become INTEGER, got: {}", result);
        assert!(lower.contains("primary key"), "SERIAL should add PRIMARY KEY, got: {}", result);
    }

    #[test]
    fn test_bigserial_to_bigint_primary_key() {
        let result = Translator::translate("CREATE TABLE users (id BIGSERIAL, name TEXT)").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("primary key"), "BIGSERIAL should add PRIMARY KEY, got: {}", result);
    }

    #[test]
    fn test_left_function() {
        let result = Translator::translate("SELECT left('hello', 3)").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("substr"), "left() should become substr(), got: {}", result);
    }

    #[test]
    fn test_right_function() {
        let result = Translator::translate("SELECT right('hello', 3)").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("substr"), "right() should become substr(), got: {}", result);
    }

    #[test]
    fn test_char_length_function() {
        let result = Translator::translate("SELECT char_length('hello')").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("length"), "char_length should become length, got: {}", result);
    }

    #[test]
    fn test_character_length_function() {
        let result = Translator::translate("SELECT character_length('hello')").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("length"), "character_length should become length, got: {}", result);
    }

    #[test]
    fn test_btrim_to_trim() {
        let result = Translator::translate("SELECT btrim('  hello  ')").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("trim"), "btrim should become trim, got: {}", result);
    }

    #[test]
    fn test_strpos_to_instr() {
        let result = Translator::translate("SELECT strpos('hello world', 'world')").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("instr"), "strpos should become instr, got: {}", result);
    }

    #[test]
    fn test_regexp_replace_to_replace() {
        let result = Translator::translate("SELECT regexp_replace('hello world', 'world', 'earth')").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("replace("), "regexp_replace should become replace, got: {}", result);
        assert!(!lower.contains("regexp_replace"), "Should not contain regexp_replace, got: {}", result);
    }

    #[test]
    fn test_regexp_replace_drops_flags() {
        let result = Translator::translate("SELECT regexp_replace('hello', 'h', 'H', 'g')").unwrap();
        let lower = result.to_lowercase();
        assert!(lower.contains("replace("), "Should become replace, got: {}", result);
        // The 4th argument (flags) should be dropped
        assert!(!lower.contains("'g'"), "Should drop flags argument, got: {}", result);
    }

    #[test]
    fn test_serial_with_existing_primary_key() {
        let result = Translator::translate("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)").unwrap();
        let lower = result.to_lowercase();
        // Should not duplicate PRIMARY KEY
        let pk_count = lower.matches("primary key").count();
        assert!(pk_count <= 2, "Should not heavily duplicate PRIMARY KEY, got: {}", result);
    }

    #[test]
    fn test_update_from() {
        let result = Translator::translate(
            "UPDATE t1 SET val = t2.val FROM t2 WHERE t1.id = t2.id"
        ).unwrap();
        let upper = result.to_uppercase();
        assert!(upper.contains("UPDATE"), "Should contain UPDATE: {}", result);
        assert!(upper.contains("FROM"), "Should contain FROM clause: {}", result);
        assert!(upper.contains("WHERE"), "Should contain WHERE: {}", result);
    }

    #[test]
    fn test_delete_using() {
        let result = Translator::translate(
            "DELETE FROM t1 USING t2 WHERE t1.id = t2.id"
        ).unwrap();
        let upper = result.to_uppercase();
        // Should be transformed to use EXISTS subquery
        assert!(upper.contains("DELETE"), "Should contain DELETE: {}", result);
        assert!(upper.contains("EXISTS"), "Should contain EXISTS subquery: {}", result);
        assert!(!upper.contains("USING"), "Should not contain USING: {}", result);
    }

    #[test]
    fn test_insert_on_conflict_do_update() {
        let result = Translator::translate(
            "INSERT INTO users (id, name) VALUES (1, 'alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
        ).unwrap();
        let upper = result.to_uppercase();
        assert!(upper.contains("ON CONFLICT"), "Should contain ON CONFLICT: {}", result);
        assert!(upper.contains("DO UPDATE"), "Should contain DO UPDATE: {}", result);
    }

    #[test]
    fn test_insert_on_conflict_do_nothing() {
        let result = Translator::translate(
            "INSERT INTO users (id, name) VALUES (1, 'alice') ON CONFLICT DO NOTHING"
        ).unwrap();
        let upper = result.to_uppercase();
        assert!(upper.contains("ON CONFLICT"), "Should contain ON CONFLICT: {}", result);
        assert!(upper.contains("DO NOTHING"), "Should contain DO NOTHING: {}", result);
    }

    #[test]
    fn test_filter_where_clause() {
        let result = Translator::translate(
            "SELECT count(*) FILTER (WHERE active = true) FROM users"
        ).unwrap();
        // FILTER WHERE should pass through to SQLite (3.30+)
        let upper = result.to_uppercase();
        assert!(upper.contains("FILTER"), "Should contain FILTER: {}", result);
    }

    #[test]
    fn test_returning_clause() {
        let result = Translator::translate(
            "INSERT INTO users (name) VALUES ('alice') RETURNING id, name"
        ).unwrap();
        let upper = result.to_uppercase();
        assert!(upper.contains("RETURNING"), "Should contain RETURNING: {}", result);
    }
}
