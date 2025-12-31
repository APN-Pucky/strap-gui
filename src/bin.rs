use core::panic;
use std::{collections::HashMap, error::Error, ops::Deref, path::{Path, PathBuf}, time::Instant};
use std::fmt::{Display, Formatter, Result as FmtResult};

use duckdb::{Connection, params};
use eframe::egui;
use egui_plot::{Bar, BarChart, Line, Plot, PlotItem, PlotPoints, Polygon};
use egui_file_dialog::FileDialog;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter};

use stattrak::StatTrack;




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
            .all(|c| c.is_ascii_alphanumeric() || ' ' == c || c == '_' || c == '/' || c == '.' || c == ':')
        {
            return Err(duckdb::Error::InvalidParameterName(format!("Invalid identifier: {}", name)));
        }

        // Safe: return the identifier as-is
        Ok(Self(name.to_string()))
    }

    /// Optionally, allow read-only access to inner string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

struct SQL {
    conn: duckdb::Connection,
    last_query: String,
    last_error: String,
}

impl SQL {
    fn prepare(&mut self, query: &str) -> duckdb::Result<duckdb::Statement> {
        self.last_query = query.to_string();
        self.conn.prepare(query)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, Display)]
enum Operation {
    Aggregate,
    Histogram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, Display)]
enum Aggregation {
    Stat,
}

struct MyApp {
    selected: String,
    operation: Operation,
    aggregation: Aggregation,
    filedialog: FileDialog,
    file : Option<PathBuf>,
    cache : Cache,

    sql : SQL,
    parquet_path : Option<ParsedString>,


    key : Option<ParsedString>,
    histogram_bins : usize,
    histogram_value_type : HistorgramValueType,
}

impl Default for MyApp {
    fn default() -> Self {
        Self {
            sql : SQL {
                conn: Connection::open_in_memory().unwrap(),
                last_query: "".to_string(),
                last_error: "".to_string(),
            },
            file : None,
            filedialog: FileDialog::new(),
            selected: "".to_string(),
            operation: Operation::Aggregate,
            aggregation: Aggregation::Stat,
            parquet_path: None,
            cache: Cache {
                histogram : HashMap::new(),
                column_names : HashMap::new(),
                stat: HashMap::new(),
            },
            key: None,
            histogram_bins: 10,
            histogram_value_type: HistorgramValueType::Count,
        }
    }
}

impl eframe::App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("STRAP GUI");

            ui.separator();

            if ui.button("Select files").clicked() {
                self.filedialog.select_file();
            }
            
            // Update the dialog
            self.filedialog.update(ctx);

            let selected = self.filedialog.selected();

            // Check if the user picked a file.
            if let Some(path) = selected {
                let file = path.to_path_buf();

                if self.file.is_none() || file.to_str() != self.file.clone().unwrap().to_str() {
                    self.parquet_path = ParsedString::parse(&format!("{}.parquet", file.to_string_lossy())).ok();
                    if let Some(parquetpath) = &self.parquet_path {
                        // if file does not end in .parquet, convert to parquet
                        if file.extension().and_then(|s| s.to_str()) != Some("parquet") {
                            if let Some (st) = StatTrack::new(&file).ok() {
                                st.to_parquet(&parquetpath).ok();
                            }
                        } else {
                            self.parquet_path = ParsedString::parse(&file.to_string_lossy()).ok();
                        }
                        self.file = Some(file); 
                    }
                    else {
                        ui.label("Faulty characters in file path");
                        return;
                    }

                }


                if let Some(parquet_path) = &self.parquet_path {
                    ui.label(format!("Loaded file: {:?}", parquet_path.as_str()));
                    ui.separator();

                    egui::ComboBox::from_label("Key")
                        .selected_text(self.key.as_ref().map_or("None", |k| k.as_str()))
                        .show_ui(ui, |ui| {
                            for name in get_column_names(&mut self.cache, &mut self.sql, ColumnNamesInput { table: parquet_path.clone() }) {
                                ui.selectable_value(&mut self.key, Some(name.clone()), name.as_str());
                            }
                    });

                    if let Some(key) = &self.key {
                        //ui.label(format!("Selected: {}", self.selected));
                        egui::ComboBox::from_label("Operation")
                            .selected_text(&self.operation.to_string())
                            .show_ui(ui, |ui| {
                                for op in Operation::iter() {
                                    ui.selectable_value(&mut self.operation, op, op.to_string());
                                }
                            });

                        match self.operation {
                            Operation::Aggregate => {
                                egui::ComboBox::from_label("Aggregation")
                                    .selected_text(&self.aggregation.to_string())
                                    .show_ui(ui, |ui| {
                                        for agg in Aggregation::iter() {
                                            ui.selectable_value(&mut self.aggregation, agg, agg.to_string());
                                        }
                                    });
                                match self.aggregation {
                                    Aggregation::Stat => {
                                        draw_stat(
                                            ui,
                                            get_stat(&mut self.cache, &mut self.sql, &StatInput {
                                                table: parquet_path.clone(),
                                                column: key.clone(),
                                            }),
                                        )
                                    },
                                }
                            },
                            Operation::Histogram => {

                                ui.add(egui::DragValue::new(&mut self.histogram_bins)
                                    .prefix("Bins: ")
                                );

                                // Add GUI for selecting histogram value type
                                egui::ComboBox::from_label("Value Type")
                                    .selected_text(&self.histogram_value_type.to_string())
                                    .show_ui(ui, |ui| {
                                        ui.selectable_value(&mut self.histogram_value_type, HistorgramValueType::Count, "Count");
                                        
                                        // For Sum and Avg, show available columns
                                        for col in get_column_names(&mut self.cache, &mut self.sql, ColumnNamesInput { table: parquet_path.clone() }) {
                                            ui.selectable_value(
                                                &mut self.histogram_value_type, 
                                                HistorgramValueType::Sum(col.clone()), 
                                                format!("Sum({})", col)
                                            );
                                        }
                                        for col in get_column_names(&mut self.cache, &mut self.sql, ColumnNamesInput { table: parquet_path.clone() }) {
                                            ui.selectable_value(
                                                &mut self.histogram_value_type, 
                                                HistorgramValueType::Avg(col.clone()), 
                                                format!("Avg({})", col)
                                            );
                                        }
                                    });

                                draw_histogram(ui, &mut self.cache, &mut self.sql, &HistogramInput {
                                    table: parquet_path.clone(),
                                    column: key.clone(),
                                    bins: self.histogram_bins,
                                    value_type: self.histogram_value_type.clone(),
                                });
                            }
                        }
                    }
                }
            }
            // Display last SQL query and error
            ui.separator();
            ui.label("Last SQL Query:");
            ui.code(&self.sql.last_query);
            ui.separator();
            ui.label("Last SQL Error:");
            ui.code(&self.sql.last_error);

        });
    }
}

fn get_column_names<'a>(cache : &'a mut Cache, sql: &mut SQL, input : ColumnNamesInput) -> &'a Vec<ParsedString> {
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
    sql: &mut SQL,
    input : &ColumnNamesInput,
) -> duckdb::Result<ColumnNamesOutput> {
    let mut stmt = sql.prepare(
        format!(
        r#"
        DESCRIBE SELECT * FROM '{}';
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
    table : ParsedString,
    column : ParsedString,
    bins: usize,
    value_type: HistorgramValueType,
}

#[derive(Hash, Eq, PartialEq, Clone, Display)]
enum HistorgramValueType {
    Count,
    #[strum(to_string = "Sum({0})")]
    Sum(ParsedString),
    #[strum(to_string = "Avg({0})")]
    Avg(ParsedString),
}

struct HistogramOutput {
    // (bin_center, count, bin_width, stddev)
    data : Vec<(f64, f64, f64, f64)>,
}


fn get_histogram<'a>(cache : &'a mut Cache, sql: &mut SQL, input : &'a HistogramInput) -> &'a HistogramOutput {
    if !cache.histogram.contains_key(&input) {
        match compute_histogram(sql, &input){
            Ok(res) => {
                cache.histogram.insert(input.clone(), res);
            },
            Err(e) => {
                sql.last_error = format!("Error computing histogram: {:?}", e);
                cache.histogram.insert(input.clone(), HistogramOutput { data : vec![] });
            }
        }
    }
    if let Some(res) = cache.histogram.get(&input) {
        res
    }
    else {
        panic!("Histogram cache miss");
    }
}

fn compute_histogram(
    sql: &mut SQL,
    hist : &HistogramInput,
) -> duckdb::Result<HistogramOutput> {
    let y_value = match &hist.value_type {
        HistorgramValueType::Count => "COUNT(*)".to_string(),
        HistorgramValueType::Sum(col) => format!("SUM({})", col),
        HistorgramValueType::Avg(col) => format!("AVG({})", col),
    };
    let y_error= match &hist.value_type {
        HistorgramValueType::Count => "SQRT(COUNT(*))".to_string(),
        HistorgramValueType::Sum(col) => format!("STDDEV({})", col),
        HistorgramValueType::Avg(col) => format!("STDDEV({})", col),
    };
    let stmt = sql.prepare(
        format!(
        r#"
SELECT
    LEAST(stats.n_bins - 1,
          CAST(FLOOR((t.{} - stats.min_val) / ((stats.max_val - stats.min_val) / stats.n_bins)) AS INTEGER)
    ) AS bucket,
    {} AS yvalue,
    {} AS yerror,
    (stats.max_val - stats.min_val) / stats.n_bins AS bin_width,
    stats.min_val + (
        (LEAST(stats.n_bins - 1,
               CAST(FLOOR((t.{} - stats.min_val) / ((stats.max_val - stats.min_val) / stats.n_bins)) AS INTEGER)
        ) + 0.5) * ((stats.max_val - stats.min_val) / stats.n_bins)
    ) AS midpoint
FROM '{}' as t
JOIN (
    SELECT MIN({}) AS min_val, MAX({}) AS max_val, {} AS n_bins
    FROM '{}'
) AS stats
ON TRUE
GROUP BY bucket, stats.min_val, stats.max_val, stats.n_bins
ORDER BY bucket;
        "#,
        hist.column,
        y_value,
        y_error,
        hist.column,
        hist.table ,
        hist.column,
        hist.column,
        hist.bins as i64,
        hist.table
    ).as_str())?.query_map(params![ ], |row| {
        Ok((
            row.get::<_, i64>(4)? as f64,
            row.get::<_, i64>(1)? as f64,
            row.get::<_, i64>(3)? as f64,
            row.get::<_, i64>(2)? as f64,
        ))
    })?
    .collect::<duckdb::Result<Vec<_>>>()?;
    Ok(HistogramOutput { data: stmt })
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct StatInput {
    table : ParsedString,
    column : ParsedString,
}

struct StatOutput {
    sum: f64,
    count: usize,
    mean: f64,
    stddev: f64,
}

fn get_stat<'a>(cache : &'a mut Cache, sql: &mut SQL, input: &StatInput) -> &'a StatOutput {
    if !cache.stat.contains_key(&input) {
        match compute_stat(sql, &input) {
            Ok(res) => {
                cache.stat.insert(input.clone(), res);
            },
            Err(e) => {
                sql.last_error = format!("Error computing stat: {:?}", e);
                cache.stat.insert(input.clone(), StatOutput { sum: 0.0, count: 0, mean: 0.0, stddev: 0.0 });
            }
        }
    }
    if let Some(res) = cache.stat.get(&input) {
        res
    }
    else {
        panic!("Stat cache miss");
    }
}

fn compute_stat(
    sql: &mut SQL,
    stat_input : &StatInput,
) -> duckdb::Result<StatOutput> {

    let stmt = sql.prepare(
        format!(
        r#"
        SELECT 
            SUM({}) as sum,
            COUNT({}) as count, 
            AVG({}) as mean,
            STDDEV({}) as stddev
        FROM '{}'
       "#,
        stat_input.column,
        stat_input.column,
        stat_input.column,
        stat_input.column,
        stat_input.table 
        ).as_str()
    )?.query_map(params![ ], |row| {
        Ok(StatOutput {
            sum: row.get(0)?,
            count: row.get(1)?,
            mean: row.get(2)?,
            stddev: row.get(3)?,
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
}


fn draw_histogram<'a>(ui: &mut egui::Ui, 
                      cache : &'a mut Cache,
                      sql: &mut SQL,
                      input : &'a HistogramInput,
    ) {
    let hist = get_histogram(cache, sql, input);
    let polygons = hist.data.iter().map(|(x, y, w, e)| {
        Polygon::new(vec![
            [x - w/2., y + e / 2.],
            [x - w/2., y - e / 2.],
            [x + w/2., y - e / 2.],
            [x + w/2., y + e / 2.],
        ])
    }).collect::<Vec<Polygon>>();


    let bars: Vec<Bar> = hist.data
        .iter()
        .map(|(x, y, w, h)| 
            Bar::new(*x, *h)
                .width(*w)
                .base_offset(y-h/2.)
                .name(format!("Value: {:.3} ± {:.3}\nRange: [{:.3}, {:.3}]\nWidth: {:.3}", 
                             y, h, x - w/2., x + w/2., w))
            )
        .collect();

    let chart = BarChart::new(bars).element_formatter(Box::new(|bar, _chart| bar.name.clone()));

    Plot::new("histogram")
        .height(300.0)
        .x_axis_label(input.table.as_str())
        .y_axis_label(match &input.value_type {
            HistorgramValueType::Count =>  "#".to_owned(),
            HistorgramValueType::Avg(col) => "Avg of ".to_owned() + col.as_str(),
            HistorgramValueType::Sum(col) => "Sum of ".to_owned() + col.as_str(),

        })
        .show(ui, |plot_ui| {
            //for polygon in polygons {
            //    plot_ui.polygon(polygon);
            //}
            plot_ui.bar_chart(chart);
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
