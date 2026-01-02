use core::panic;
use std::{collections::HashMap, fmt::{self}, ops::Deref};
use std::fmt::{Display, Formatter, Result as FmtResult};

use duckdb::{Connection, params};
use eframe::egui;
use egui_plot::{Bar, BarChart, Legend, Plot};
use egui_file_dialog::FileDialog;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter};

use straptrack::StrapTrack;

#[derive(Hash, Eq, PartialEq, Clone)]
struct SQLFilter {
    // Each Vec<SQLFilterComparison> is an OR group
    // All groups must be satisfied (AND between groups)
    conditions  : Vec<Vec<SQLFilterComparison >>,
}

impl SQLFilter {
    fn is_empty(&self) -> bool {
        self.conditions.is_empty() || self.conditions.iter().all(|group| group.is_empty())
    }

    fn to_sql(&self) -> String {
        self.conditions.iter().map(|group| {
            "(".to_string()
            + group.iter().map(|c| c.to_sql()).collect::<Vec<_>>().join(" OR ").as_str()
            + ")"
        }).collect::<Vec<_>>().join(" AND ")
    }

    fn to_sql_and_prefix(&self) -> String {
        let mut query = String::new();
        if !self.is_empty() {
            query.push_str(" AND ");
            query.push_str(self.to_sql().as_str());
        }
        query
    }

    fn to_sql_where_prefix(&self) -> String {
        let mut query = String::new();
        if !self.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(self.to_sql().as_str());
        }
        query
    }
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct SQLFilterComparison {
    left: SQLFilterComparisonValue,
    comparison: SQLFilterComparisonOperation,
    right: SQLFilterComparisonValue,
}

#[derive(Hash, Eq, PartialEq, Clone)]
enum SQLFilterComparisonValue {
    Column(ParsedString),
    Number(String),
}

impl fmt::Display for SQLFilterComparisonValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column(col) => write!(f, "{}", col),
            Self::Number(num) => write!(f, "{}", num),
        }
    }
}

impl SQLFilterComparison {
    fn to_sql(&self) -> String {
        format!("{} {} {}", self.left, self.comparison, self.right)
    }
}

#[derive(Hash, Eq, PartialEq, Clone, EnumIter)]
enum SQLFilterComparisonOperation {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

impl fmt::Display for SQLFilterComparisonOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::GreaterThan => ">",
            Self::LessThan => "<",
            Self::GreaterThanOrEqual => ">=",
            Self::LessThanOrEqual => "<=",
        };
        write!(f, "{s}")
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedString(String);

impl Deref for ParsedString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Display for ParsedString {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}
impl ParsedString {
    fn parse(name: &str) -> duckdb::Result<ParsedString> {
        // Allow only letters, numbers, slash, double dot and underscores
        if name.is_empty() {
            return Err(duckdb::Error::InvalidParameterName("Identifier cannot be empty".to_owned()));
        }

        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || ' ' == c|| c == '-' || c == '_' || c == '/' || c == '.' || c == ':')
        {
            return Err(duckdb::Error::InvalidParameterName(format!("Invalid identifier: {}", name)));
        }

        // Safe: return the identifier as-is
        Ok(Self("\"".to_string() + name + "\""))
    }

    /// Optionally, allow read-only access to inner string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

struct Sql {
    conn: duckdb::Connection,
    last_query: String,
    last_error: String,
}

impl Sql {
    fn prepare(&mut self, query: &str) -> duckdb::Result<duckdb::Statement> {
        self.last_query = query.to_string();
        self.conn.prepare(query)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, Display)]
enum Operation {
    //Aggregate,
    Histogram,
}

struct MyApp {
    operation: Operation,
    filedialog: FileDialog,
    cache : Cache,

    sql : Sql,


    histogram_view : HistogramView,
    global_id_counter: usize,
}

struct HistogramView {
    //bin_scale: HistogramBinScale,
    plot_settings : HistrogramPlotSettings,
    input : HistogramInput,
}

struct HistrogramPlotSettings {
    //x_axis_scale: HistogramAxisScale,
    //y_axis_scale: HistogramAxisScale,
}


impl Default for MyApp {
    fn default() -> Self {
        Self {
            sql : Sql {
                conn: Connection::open_in_memory().unwrap(),
                last_query: "".to_string(),
                last_error: "".to_string(),
            },
            filedialog: FileDialog::new(),
            operation: Operation::Histogram,
            cache: Cache {
                histogram : HashMap::new(),
                column_names : HashMap::new(),
                stat: HashMap::new(),
            },
            histogram_view : HistogramView {
                plot_settings : HistrogramPlotSettings {
                //    x_axis_scale: HistogramAxisScale::Linear,
                //    y_axis_scale: HistogramAxisScale::Linear,
                },
                input : HistogramInput {
                    bins: 10,
                    curves : vec![],
                },
                //bin_scale: HistogramBinScale::Linear,
            },
            global_id_counter: 0,
        }
    }
}

impl eframe::App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::both().show(ui, |ui| {
                ui.set_min_width(ui.available_width());
                ui.heading("STRAP GUI");

                ui.separator();

                ui.horizontal(|ui| {
                    for op in Operation::iter() {
                        ui.selectable_value(&mut self.operation, op, op.to_string());
                    }
                });

                ui.separator();

                match self.operation {
                    Operation::Histogram => {
                        //ui.horizontal(|ui| {
                        //    ui.label("Histogram X Axis Scale: ");
                        //    for op in HistogramAxisScale::iter() {
                        //        ui.selectable_value(&mut self.histogram_view.plot_settings.x_axis_scale, op, op.to_string());
                        //    }
                        //});
                        //ui.horizontal(|ui| {
                        //    ui.label("Histogram Y Axis Scale: ");
                        //    for op in HistogramAxisScale::iter() {
                        //        ui.selectable_value(&mut self.histogram_view.plot_settings.y_axis_scale, op, op.to_string());
                        //    }
                        //});
                        //ui.horizontal(|ui| {
                        //    ui.label("Histogram Bin Scale: ");
                        //    for op in HistogramBinScale::iter() {
                        //        ui.selectable_value(&mut self.histogram_view.bin_scale, op, op.to_string());
                        //    }
                        //});
                        ui.horizontal(|ui| {
                            ui.label("Histogram Bins: ");
                            ui.add(egui::DragValue::new(&mut self.histogram_view.input.bins));
                        });

                        ui.separator();

                        if ui.button("Add Histogram").clicked() {
                            self.filedialog.select_file();
                        };

                        // Update the dialog
                        self.filedialog.update(ctx);

                        if let Some(path) = self.filedialog.selected(){
                            let file = path.to_path_buf();
                            self.filedialog = FileDialog::new();
                            let parquet_path = 
                                // if file does not end in .parquet, convert to parquet
                                if file.extension().and_then(|s| s.to_str()) != Some("parquet") {
                                    let pp = format!("{}.parquet", file.to_string_lossy());
                                    let mut parquet_path = ParsedString::parse(&pp).ok();
                                    if parquet_path.is_some() 
                                        && let Ok(st) = StrapTrack::new(&file)
                                        && st.to_parquet(&pp, 1000).is_err()
                                    {
                                        // error converting to parquet
                                        ui.label("Error converting to parquet");
                                        parquet_path = None
                                    }
                                    parquet_path
                                } else {
                                    ParsedString::parse(&file.to_string_lossy()).ok()
                                };
                                if let Some(parquetpath) = &parquet_path {
                                    let columns = get_column_names(&mut self.cache, &mut self.sql, ColumnNamesInput { table: parquetpath.clone() });
                                    if columns.is_empty() {
                                        ui.label("No columns found in file");
                                        return;
                                    }
                                    let key = columns.first();
                                    if let Some(key) = key {
                                        self.global_id_counter += 1;
                                        self.histogram_view.input.curves.push(HistogramSubInput {
                                            id : self.global_id_counter,
                                            table: parquetpath.clone(),
                                            filter: SQLFilter { conditions: vec![] },
                                            x_key: key.clone(),
                                            value_type: HistogramAggregation::Count,
                                            y_key: key.clone(),
                                        });
                                    }
                                    else {
                                        ui.label("No valid columns found in file");
                                        return;
                                    }
                                }
                                else {
                                    ui.label("Faulty characters in file path");
                                    return;
                                }
                        }

                        let mut curves_to_clone = Vec::new();
                        let mut curves_to_remove = Vec::new();

                        ui.horizontal(|ui| {
                            for curve in &mut self.histogram_view.input.curves {
                                ui.vertical(|ui| {
                                    ui.horizontal(|ui| {
                                        if ui.button("Clone").clicked() {
                                            curves_to_clone.push(curve.clone());
                                        }
                                        if ui.button("Remove").clicked() {
                                            curves_to_remove.push(curve.clone());
                                        }
                                    });
                                    let parquet_path = &curve.table;
                                    let columns = get_column_names(&mut self.cache, &mut self.sql, ColumnNamesInput { table: parquet_path.clone() });
                                    let filename = curve.table.as_str()
                                        .trim_matches('"')
                                        .split('/')
                                        .next_back()
                                        .unwrap_or("unknown")
                                        .replace(".parquet", "");
                                    ui.label( filename.to_string());

                                        egui::ComboBox::new(format!("x_key_{}", curve.id) ,"X Key")
                                            .selected_text(curve.x_key.as_str())
                                            .show_ui(ui, |ui| {
                                                for name in columns {
                                                    ui.selectable_value(&mut curve.x_key, name.clone(), name.as_str());
                                                }
                                        });

                                        egui::ComboBox::new(format!("y_key_{}", curve.id) ,"Y Key")
                                            .selected_text(curve.y_key.as_str())
                                            .show_ui(ui, |ui| {
                                                for name in columns {
                                                    ui.selectable_value(&mut curve.y_key, name.clone(), name.as_str());
                                                }
                                        });

                                        egui::ComboBox::new(format!("type_{}", curve.id),"Type")
                                            .selected_text(curve.value_type.to_string())
                                            .show_ui(ui, |ui| {
                                                for name in HistogramAggregation::iter() {
                                                    ui.selectable_value(&mut curve.value_type, name, name.to_string());
                                                }
                                        });

                                        // Add expandable filter section
                                        egui::CollapsingHeader::new("Filters")
                                            .id_source(format!("filters_{}", curve.id))
                                            .default_open(true)
                                            .show(ui, |ui| {
                                                // Add new filter group button
                                                if ui.button("Add Filter Group").clicked() {
                                                    curve.filter.conditions.push(vec![]);
                                                }

                                                let mut groups_to_remove = Vec::new();

                                                for (group_idx, group) in curve.filter.conditions.iter_mut().enumerate() {
                                                    ui.horizontal(|ui| {
                                                        ui.label(format!("OR Group {}", group_idx + 1));
                                                        if ui.button("Remove Group").clicked() {
                                                            groups_to_remove.push(group_idx);
                                                        }
                                                    });

                                                    ui.indent(format!("group_{}", group_idx), |ui| {
                                                        // Add condition to group button
                                                        if ui.button("Add Condition").clicked() {
                                                            group.push(SQLFilterComparison {
                                                                left: SQLFilterComparisonValue::Number("0".to_string()),
                                                                comparison: SQLFilterComparisonOperation::Equal,
                                                                right: SQLFilterComparisonValue::Number("0".to_string()),
                                                            });
                                                        }

                                                        let mut conditions_to_remove = Vec::new();

                                                        for (cond_idx, condition) in group.iter_mut().enumerate() {
                                                            ui.horizontal(|ui| {

                                                                if ui.small_button("x").clicked() {
                                                                    conditions_to_remove.push(cond_idx);
                                                                }

                                                                let mut is_column = matches!(condition.right, SQLFilterComparisonValue::Column(_));

                                                                ui.checkbox(&mut is_column, "Column");


                                                                // Left side (column selection)
                                                                egui::ComboBox::new(format!("left_{}_{}", group_idx, cond_idx), "")
                                                                    .selected_text(condition.left.to_string())
                                                                    .show_ui(ui, |ui| {
                                                                        for col in columns {
                                                                            ui.selectable_value(&mut condition.left, SQLFilterComparisonValue::Column(col.clone()), col.as_str());
                                                                        }
                                                                    });
                                                                
                                                                // Comparison operator
                                                                egui::ComboBox::new(format!("op_{}_{}", group_idx, cond_idx), "")
                                                                    .selected_text(condition.comparison.to_string())
                                                                    .show_ui(ui, |ui| {
                                                                        for op in SQLFilterComparisonOperation::iter() {
                                                                            ui.selectable_value(&mut condition.comparison, op.clone(), op.to_string());
                                                                        }
                                                                    });

                                                                if is_column {
                                                                    if let SQLFilterComparisonValue::Number(_) = condition.right {
                                                                        // Reset to first column if previously a number
                                                                        condition.right = SQLFilterComparisonValue::Column(columns.first().cloned().unwrap_or(ParsedString::parse("0").unwrap()));
                                                                    }
                                                                    // Column selection dropdown
                                                                    let current_col = match &condition.right {
                                                                        SQLFilterComparisonValue::Column(col) => col.as_str(),
                                                                        SQLFilterComparisonValue::Number(_) => columns.first().map(|c| c.as_str()).unwrap_or(""),
                                                                    };
        
                                                                    egui::ComboBox::new(format!("right_col_{}_{}", group_idx, cond_idx),"")
                                                                        .selected_text(current_col)
                                                                        .show_ui(ui, |ui| {
                                                                            for col in columns {
                                                                                ui.selectable_value(&mut condition.right, SQLFilterComparisonValue::Column(col.clone()), col.as_str());
                                                                            }
                                                                        });
                                                                }
                                                                else {
                                                                    if let SQLFilterComparisonValue::Column(_) = condition.right {
                                                                        // Reset to 0 if previously a column
                                                                        condition.right = SQLFilterComparisonValue::Number("0".to_string());
                                                                    }
                                                                    // Right side is a number
                                                                    let mut value_text = if let SQLFilterComparisonValue::Number(ref num) = condition.right {
                                                                        num.clone()
                                                                    } else {
                                                                        "0".to_string()
                                                                    };
                                                                    if ui.add(
                                                                        egui::TextEdit::singleline(&mut value_text)
                                                                            .desired_width(50.0)
                                                                    ).changed() {
                                                                        if let Ok(v) = value_text.parse::<f64>() {
                                                                            // Valid number
                                                                            condition.right = SQLFilterComparisonValue::Number(v.to_string());
                                                                        }
                                                                        else {
                                                                            // Invalid number, reset to 0
                                                                            condition.right = SQLFilterComparisonValue::Number("0".to_string());
                                                                        }
                                                                    }
                                                                }
                                                                
                                                            });

                                                            //if cond_idx < group.len() - 1 {
                                                            //    ui.label("OR");
                                                            //}
                                                        }

                                                        // Remove conditions in reverse order
                                                        for &idx in conditions_to_remove.iter().rev() {
                                                            group.remove(idx);
                                                        }
                                                    });

                                                    //if group_idx < curve.filter.conditions.len() - 1 {
                                                    //    ui.label("AND");
                                                    //}
                                                }

                                                // Remove groups in reverse order
                                                for &idx in groups_to_remove.iter().rev() {
                                                    curve.filter.conditions.remove(idx);
                                                }

                                                // Show current filter SQL
                                                if !curve.filter.conditions.is_empty() {
                                                    ui.label("Current filter:");
                                                    ui.code(curve.filter.to_sql());
                                                }
                                            });

    
    
                                        //ui.label(format!("Selected: {}", self.selected));
                                        draw_stat(
                                            ui,
                                            get_stat(&mut self.cache, &mut self.sql, &StatInput {
                                                table: parquet_path.clone(),
                                                column: curve.x_key.clone(),
                                                filters: curve.filter.clone(),
                                        }),
                                    );
                                });
                            }
                        });

                        for curve in curves_to_clone {
                            let mut nc = curve.clone();
                            nc.id = {
                                self.global_id_counter += 1;
                                self.global_id_counter
                            };
                            self.histogram_view.input.curves.push(nc);
                        }
                        for curve in curves_to_remove {
                            self.histogram_view.input.curves.retain(|x| *x != curve);
                        }


                        draw_histogram(ui, &mut self.cache, &mut self.sql, &self.histogram_view.input, &self.histogram_view.plot_settings);
                    }
                }

                ui.separator();
                // Display last SQL query and error
                egui::CollapsingHeader::new("Last SQL Query:")
                    .default_open(true) // collapsed by default
                    .show(ui, |ui| {
                        ui.code(&self.sql.last_query);
                    });
                
                ui.separator();
                
                egui::CollapsingHeader::new("Last SQL Error:")
                    .default_open(true) // collapsed by default
                    .show(ui, |ui| {
                        ui.code(&self.sql.last_error);
                    });
            });
        });
    }
}

fn get_column_names<'a>(cache : &'a mut Cache, sql: &mut Sql, input : ColumnNamesInput) -> &'a Vec<ParsedString> {
    if ! cache.column_names.contains_key(&input) {
        match compute_column_names(sql, &input) {
            Ok(res) => {
                cache.column_names.insert(input.clone(), res);
            },
            Err(e) => {
                sql.last_error = format!("Error computing column names: {:?}", e);
                cache.column_names.insert(input.clone(), ColumnNamesOutput { names : vec![] });
            }
        }
    }
    if let Some(res) = cache.column_names.get(&input) {
        &res.names
    }
    else {
        panic!("Column names cache miss");
    }
}

fn compute_column_names(
    sql: &mut Sql,
    input : &ColumnNamesInput,
) -> duckdb::Result<ColumnNamesOutput> {
    let mut stmt = sql.prepare(
        format!(
        r#"
        DESCRIBE SELECT * FROM {};
       "#,&input.table.as_str()
        ).as_str()
    )?;
    let column_names = stmt.query_map(params![], |row| {
        ParsedString::parse(&row.get::<_, String>(0)?)
        //Ok(row.get::<_, String>(1)?)
    })?
    .collect::<duckdb::Result<Vec<_>>>()?;
    Ok(ColumnNamesOutput { names: column_names })
}

struct ColumnNamesOutput {
    names : Vec<ParsedString>,
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct ColumnNamesInput {
    table : ParsedString,
}


struct Cache {
    column_names : HashMap<ColumnNamesInput, ColumnNamesOutput>,
    histogram : HashMap<HistogramInput, HistogramOutput>,
    stat : HashMap<StatInput, StatOutput>,
}


#[derive(Hash, Eq, PartialEq, Clone)]
struct HistogramInput {
    bins: usize,
    curves : Vec<HistogramSubInput>,
}


#[derive(Hash, Eq, PartialEq, Clone)]
struct HistogramSubInput {
    id : usize,
    table : ParsedString,
    filter : SQLFilter,
    x_key : ParsedString,
    value_type: HistogramAggregation,
    y_key : ParsedString,
}

#[derive(Copy, Hash, Eq, PartialEq, Clone, Display,EnumIter)]
enum HistogramAggregation{
    Count,
    Sum,
    Avg,
}

//#[derive(Copy, Hash, Eq, PartialEq, Clone, Display,EnumIter)]
//enum HistogramAxisScale {
//    Linear,
//    //Log, // egui plot not supported yet: https://github.com/emilk/egui_plot/pull/29
//}

//#[derive(Copy, Hash, Eq, PartialEq, Clone, Display,EnumIter)]
//enum HistogramBinScale{
//    Linear,
//    //Log, // TODO SQL
//}

//#[derive(Hash, Eq, PartialEq, Clone, Display)]
//enum HistorgramValueType {
//    #[strum(to_string = "Count({0})")]
//    Count(ParsedString),
//    #[strum(to_string = "Sum({0})")]
//    Sum(ParsedString),
//    #[strum(to_string = "Avg({0})")]
//    Avg(ParsedString),
//}

struct HistogramOutput {
    // (bin_center, bin_width, count, stddev)
    data : Vec<(f64, f64, Vec<(f64, f64)>)>,
}


fn get_histogram<'a>(cache : &'a mut Cache, sql: &mut Sql, input : &'a HistogramInput) -> &'a HistogramOutput {
    if !cache.histogram.contains_key(input) {
        match compute_histogram(sql, input){
            Ok(res) => {
                cache.histogram.insert(input.clone(), res);
            },
            Err(e) => {
                sql.last_error = format!("Error computing histogram: {:?}", e);
                cache.histogram.insert(input.clone(), HistogramOutput { data : vec![] });
            }
        }
    }
    if let Some(res) = cache.histogram.get(input) {
        res
    }
    else {
        panic!("Histogram cache miss");
    }
}

fn compute_histogram(
    sql: &mut Sql,
    hist : &HistogramInput,
) -> duckdb::Result<HistogramOutput> {
    let mut filters:String= String::new();
    let mut hists = Vec::new();
    let mut coalesced = String::new();
    let mut joins = String::new();
    for (i, c) in hist.curves.iter().enumerate() {
        let y_value = match c.value_type {
            HistogramAggregation::Count => format!("COUNT({})", c.y_key),
            HistogramAggregation::Sum => format!("SUM({})", c.y_key),
            HistogramAggregation::Avg => format!("AVG({})", c.y_key),
        };
        let y_error= match c.value_type {
            HistogramAggregation::Count => format!("SQRT(COUNT({}))", c.y_key),
            HistogramAggregation::Sum => format!("STDDEV({})", c.y_key),
            HistogramAggregation::Avg => format!("STDDEV({})", c.y_key),
        };
        filters.push_str(
            format!(
                r#"
filtered_{} AS (
    SELECT *
    FROM {}
    WHERE ( {} IS NOT NULL AND {} IS NOT NULL ) {} 
),
                "#,i, c.table.as_str(), c.x_key.as_str(), c.y_key.as_str(), c.filter.to_sql_and_prefix()
            ).as_str()
        );

        hists.push(
            format!(
                r#"
hist_{} AS (
    SELECT 
        LEAST(stats.n_bins - 1,
              CAST(FLOOR((t.{} - stats.min_val) / ((stats.max_val - stats.min_val) / stats.n_bins)) AS INTEGER)
        ) AS bucket,
        {} AS yvalue,
        {} AS yerror,
    FROM filtered_{} as t
    JOIN stats ON TRUE
    GROUP BY bucket
)
                "#,i, c.x_key.as_str(), y_value, y_error, i
            ).to_string()
        );
        coalesced.push_str(
            format!(
                r#"
                COALESCE(h{}.yvalue, 0) AS yvalue_{},
                COALESCE(h{}.yerror, 0) AS yerror_{},
                "#, i, i, i, i
            ).as_str()
        );
        joins.push_str(
            format!(
                r#"
LEFT JOIN hist_{} AS h{} ON h{}.bucket = b.bucket
                "#, i, i, i
            ).as_str()
        );                
    }
    let x_keys = hist.curves.iter().map(|c| c.x_key.as_str()).collect::<Vec<_>>().join(", ");
    let combined = hist.curves.iter().enumerate().map(|(i, _c)| 
            format!(
                r#"
SELECT * FROM filtered_{}

                "#, i
            ).to_string()
        ).collect::<Vec<_>>().join("UNION ALL");
    let mid = format!(
        r#"
stats AS (
    SELECT 
        MIN(LEAST({})) AS min_val,
        MAX(GREATEST({})) AS max_val,
    {} AS n_bins
    FROM combined
),
buckets AS (
    SELECT
        g.bucket,
        stats.min_val +
        (g.bucket + 0.5) * ((stats.max_val - stats.min_val) / stats.n_bins)
        AS midpoint,
        (stats.max_val - stats.min_val) / stats.n_bins AS width
    FROM stats
    JOIN generate_series(0, stats.n_bins - 1) AS g(bucket)
    ON TRUE
),
        "#,
        x_keys,
        x_keys,
        hist.bins as i64
    );
    let stmt = sql.prepare(
        format!(
        r#"
WITH
        {}
combined AS (
        {}
),
        {}
        {}
SELECT
    b.bucket,
    b.midpoint,
    b.width,
    {}
FROM buckets AS b
        {}
ORDER BY b.bucket
        "#,filters, combined, mid, hists.join(","),coalesced, joins
    ).as_str())?.query_map(params![ ], |row| {
        let bin_center = row.get::<_, f64>(1)?;
        let bin_width = row.get::<_, f64>(2)?;
        let mut values = Vec::new();
        let n_curves = hist.curves.len();
        for i in 0..n_curves {
            let y_value = row.get::<_, f64>(3 + i * 2)?;
            let y_error = row.get::<_, f64>(4 + i * 2)?;
            values.push((y_value, y_error));
        }
        Ok((
            bin_center,
            bin_width,
            values,
        ))
    })?
    .collect::<duckdb::Result<Vec<_>>>()?;
    Ok(HistogramOutput { data: stmt })
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct StatInput {
    table : ParsedString,
    column : ParsedString,
    filters : SQLFilter,
}

struct StatOutput {
    sum: f64,
    count: usize,
    mean: f64,
    stddev: f64,
    min : f64,
    max : f64,
}

fn get_stat<'a>(cache : &'a mut Cache, sql: &mut Sql, input: &StatInput) -> &'a StatOutput {
    if !cache.stat.contains_key(input) {
        match compute_stat(sql, input) {
            Ok(res) => {
                cache.stat.insert(input.clone(), res);
            },
            Err(e) => {
                sql.last_error = format!("Error computing stat: {:?}", e);
                cache.stat.insert(input.clone(), StatOutput { sum: 0.0, count: 0, mean: 0.0, stddev: 0.0, min: 0.0, max: 0.0 });
            }
        }
    }
    if let Some(res) = cache.stat.get(input) {
        res
    }
    else {
        panic!("Stat cache miss");
    }
}

fn compute_stat(
    sql: &mut Sql,
    stat_input : &StatInput,
) -> duckdb::Result<StatOutput> {

    let stmt = sql.prepare(
        format!(
        r#"
        SELECT 
            SUM(t.{}) as sum,
            COUNT(t.{}) as count, 
            AVG(t.{}) as mean,
            STDDEV(t.{}) as stddev,
            MIN(t.{}) as min,
            MAX(t.{}) as max
        FROM {} AS t
        {}
       "#,
        stat_input.column,
        stat_input.column,
        stat_input.column,
        stat_input.column,
        stat_input.column,
        stat_input.column,
        stat_input.table ,
        stat_input.filters.to_sql_where_prefix()
        ).as_str()
    )?.query_map(params![ ], |row| {
        Ok(StatOutput {
            sum: row.get(0)?,
            count: row.get(1)?,
            mean: row.get(2)?,
            stddev: row.get(3)?,
            min: row.get(4)?,
            max: row.get(5)?,
        })
    })?
    .next();

    if let Some(stat) = stmt {
        stat
    } else {
        Err(duckdb::Error::QueryReturnedNoRows)
    }

}

fn draw_stat(ui: &mut egui::Ui, stat : & StatOutput ) {
    ui.label(format!("Sum: {:.4}", stat.sum));
    ui.label(format!("Count: {}", stat.count));
    ui.label(format!("Mean: {:.4}", stat.mean));
    ui.label(format!("Std Dev: {:.4}", stat.stddev));
    ui.label(format!("Min: {:.4}", stat.min));
    ui.label(format!("Max: {:.4}", stat.max));
}

fn transpose<T: Clone>(matrix: Vec<Vec<T>>) -> Vec<Vec<T>> {
    if matrix.is_empty() || matrix[0].is_empty() {
        return vec![];
    }

    let n = matrix.len();
    let m = matrix[0].len();

    (0..m)
        .map(|i| (0..n).map(|j| matrix[j][i].clone()).collect())
        .collect()
}

fn draw_histogram<'a>(ui: &mut egui::Ui, 
                      cache : &'a mut Cache,
                      sql: &mut Sql,
                      input : &'a HistogramInput,
                      plot_settings: &HistrogramPlotSettings,
    ) {
    if input.curves.is_empty() {
        ui.label("No histogram curves to display");
        return;
    }
    let hist = get_histogram(cache, sql, input);
    let bars: Vec<Vec<Bar>> = transpose(hist.data
        .iter()
        .map(|(x,w , values)| 
            values.iter().map(|(y, h)| {
                Bar::new(*x, *h)
                    .width(*w)
                    .base_offset(y-h/2.)
                    .name(format!("Value: {:.3} ± {:.3}\nRange: [{:.3}, {:.3}]\nWidth: {:.3}", 
                                 y, h, x - w/2., x + w/2., w))
                } ).collect()
            )
        .collect());

    // add names
    let charts: Vec<BarChart> = bars.iter()
    .enumerate()
    .map(|(i, bar_group)| {
        let curve = &input.curves[i];
        // Extract just the filename without path and extension
        let filename = curve.table.as_str()
            .trim_matches('"')
            .split('/')
            .next_back()
            .unwrap_or("unknown")
            .replace(".parquet", "");
        let legend_name = format!("{}. {} of {} vs {} ({})", 
                                 i + 1,
                                 curve.value_type, 
                                 curve.y_key.as_str().trim_matches('"'), 
                                 curve.x_key.as_str().trim_matches('"'),
                                 filename);
                            
        
        BarChart::new(bar_group.clone())
            .name(legend_name)  // Each curve gets its own descriptive name
            .element_formatter(Box::new(|bar, _chart| bar.name.clone()))
    }).collect();


    Plot::new("histogram")
        .height(400.0)
        .legend(Legend::default())
        .x_axis_label(
            input.curves.iter().map(|c| c.x_key.as_str()).collect::<Vec<_>>().as_slice().join(" / ")
        )
        // TODO move axis labels to legend
        .y_axis_label(
            input.curves.iter().map(|c| 
                match c.value_type {
                    HistogramAggregation::Count => "COUNT(".to_owned() +c.y_key.as_str() + ")",
                    HistogramAggregation::Avg => "AVG(".to_owned() + c.y_key.as_str() + ")",
                    HistogramAggregation::Sum => "SUM(".to_owned() + c.y_key.as_str() + ")",
                }
            ).collect::<Vec<_>>().as_slice().join(" / ")
            )
        .show(ui, |plot_ui| {
            for chart in charts {
                plot_ui.bar_chart(chart);
            }
        });
}

fn main() -> Result<(), eframe::Error> {
    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "STRAP GUI",
        options,
        Box::new(|_| Box::new(MyApp::default())),
    )
}
