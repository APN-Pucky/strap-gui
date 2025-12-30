use core::panic;
use std::{collections::HashMap, path::{Path, PathBuf}};

use eframe::egui;
use egui_plot::{Bar, BarChart, Line, Plot, PlotPoints};
use polars_lazy::{dsl::{col, count, lit}, frame::LazyFrame};
use rand::Rng;
use egui_file_dialog::FileDialog;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter};
use polars::prelude::*;

use stattrak::StatTrack;

// TODO filter, 2d plots, move key to operations, histogram
// gzip support

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
    lf : Option<LazyFrame>,

    histogram_input : HistogramInput,
    stat_input : StatInput,
}

impl Default for MyApp {
    fn default() -> Self {
        Self {
            file : None,
            filedialog: FileDialog::new(),
            selected: "".to_string(),
            operation: Operation::Aggregate,
            aggregation: Aggregation::Stat,
            cache: Cache {
                histogram : HashMap::new(),
                column_names : vec![],
                stat: HashMap::new(),
            },
            lf: None,
            histogram_input: HistogramInput {
                column: "".to_string(),
                bins: 10,
            },
            stat_input: StatInput {
                column: "".to_string(),
            },
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
                ui.label(format!("Loaded file: {:?}", file));

                if self.file.is_none() || file.to_str() != self.file.clone().unwrap().to_str() {
                    let mut parquet_path = format!("{}.parquet", file.to_string_lossy());
                    // if file does not end in .parquet, convert to parquet
                    if file.extension().and_then(|s| s.to_str()) != Some("parquet") {
                        if let Some (st) = StatTrack::new(&file).ok() {
                            st.to_parquet(&parquet_path).ok();
                        }
                    } else {
                        parquet_path = file.to_string_lossy().to_string();
                    }
                    self.file = Some(file); 
                    self.lf = LazyFrame::scan_parquet(&parquet_path, ScanArgsParquet::default()).ok();
                }

                ui.separator();

                if let Some(lf ) = &self.lf {
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

                                    egui::ComboBox::from_label("Key")
                                        .selected_text(&self.stat_input.column.clone())
                                        .show_ui(ui, |ui| {
                                            for name in get_column_names(&mut self.cache,&lf) {
                                                ui.selectable_value(&mut self.stat_input.column, name.clone(), name);
                                            }
                                    });
                                    draw_stat(
                                        ui,
                                        get_stat(&mut self.cache, &lf, &self.stat_input)
                                    )
                                },
                            }
                        },
                        Operation::Histogram => {
                            egui::ComboBox::from_label("Key")
                                .selected_text(&self.histogram_input.column.clone())
                                .show_ui(ui, |ui| {
                                    for name in get_column_names(&mut self.cache,&lf) {
                                        ui.selectable_value(&mut self.histogram_input.column, name.clone(), name);
                                    }
                            });
                            ui.add(egui::DragValue::new(&mut self.histogram_input.bins).suffix("Bins"));

                            draw_histogram(ui, get_histogram(&mut self.cache, &lf, &self.histogram_input));
                        }
                    }
                }
            }
        });
    }
}

fn get_column_names<'a>(cache : &'a mut Cache, lf: &'a  LazyFrame) -> &'a Vec<String> {
    if cache.column_names.is_empty() {
        let cols = lf.schema().unwrap().iter_names().map(|s| s.to_string()).collect();
        cache.column_names = cols;
    }
    &cache.column_names
}


struct Cache {
    histogram : HashMap<HistogramInput, HistogramOutput>,
    stat : HashMap<StatInput, StatOutput>,
    column_names : Vec<String>,
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct HistogramInput {
    column : String,
    bins: usize,
}

struct HistogramOutput {
    data : Vec<(f64, f64)>,
    width : f64,
}


fn get_histogram<'a>(cache : &'a mut Cache, lf: &'a  LazyFrame,  input : &'a HistogramInput) -> &'a HistogramOutput {
    if !cache.histogram.contains_key(&input) {
        if let Ok(res) = compute_histogram(lf, &input) {
            cache.histogram.insert(input.clone(), res);
        }
        else {
            cache.histogram.insert(input.clone(), HistogramOutput { data : vec![], width: 0.0 });
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
    lf: &LazyFrame,
    hist : &HistogramInput,
) -> PolarsResult<HistogramOutput> {
    // Compute min/max
    let stats = lf.clone()
        .select([
            col(&hist.column).min().alias("min"),
            col(&hist.column).max().alias("max"),
        ])
        .collect()?;

    let min = stats.column("min")?.get(0)?.try_extract::<f64>()?;
    let max = stats.column("max")?.get(0)?.try_extract::<f64>()?;

    let bin_width = (max - min) / hist.bins as f64;
    println!("Min: {}, Max: {}, Bin width: {}", min, max, bin_width);

    // Assign bin index
    let hist = lf.clone()
    .select([
        ((col(&hist.column) - lit(min)) / lit(bin_width))
            .floor()
            .cast(DataType::Int64)
            .clip(AnyValue::Int64(0), AnyValue::Int64(hist.bins as i64 - 1)) // clamp between min and max bin
            .alias("bin"),
    ])
        .groupby([col("bin")])
        .agg([count().alias("count")])
        .sort("bin", Default::default())
        .collect()?;

    // Convert to (bin_center, count)
    let bin_col = hist.column("bin")?.i64()?;
    let count_col = hist.column("count")?.u32()?;

    let mut out = Vec::new();
    for (bin, count) in bin_col.into_iter().zip(count_col) {
        if let (Some(b), Some(c)) = (bin, count) {
            let center = min + (b as f64 + 0.5) * bin_width;
            out.push((center, c as f64));
        }
    }

    Ok(HistogramOutput { data: out, width: bin_width })
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct StatInput {
    column : String,
}

struct StatOutput {
    sum: f64,
    count: usize,
    mean: f64,
    stddev: f64,
}

fn get_stat<'a>(cache : &'a mut Cache, lf: &'a  LazyFrame, input: &StatInput) -> &'a StatOutput {
    if !cache.stat.contains_key(&input) {
        if let Ok(res) = compute_stat(lf, &input) {
            cache.stat.insert(input.clone(), res);
        }
        else {
            cache.stat.insert(input.clone(), StatOutput { sum: 0.0, count: 0, mean: 0.0, stddev: 0.0 });
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
    lf: &LazyFrame,
    stat_input : &StatInput,
) -> PolarsResult<StatOutput> {
    let stats = lf.clone()
        .select([
            col(&stat_input.column).sum().alias("sum"),
            col(&stat_input.column).count().alias("count"),
            col(&stat_input.column).mean().alias("mean"),
            col(&stat_input.column).std(1).alias("stddev"),
        ])
        .collect()?;

    let sum = stats.column("sum")?.get(0)?.try_extract::<f64>()?;
    let count = stats.column("count")?.get(0)?.try_extract::<u32>()? as usize;
    let mean = stats.column("mean")?.get(0)?.try_extract::<f64>()?;
    let stddev = stats.column("stddev")?.get(0)?.try_extract::<f64>()?;

    Ok(StatOutput { sum, count, mean, stddev })
}

fn draw_stat(ui: &mut egui::Ui, stat : & StatOutput ) {
    ui.label(format!("Sum: {:.4}", stat.sum));
    ui.label(format!("Count: {}", stat.count));
    ui.label(format!("Mean: {:.4}", stat.mean));
    ui.label(format!("Std Dev: {:.4}", stat.stddev));
}


fn draw_histogram(ui: &mut egui::Ui, hist : &HistogramOutput ) {
    let bars: Vec<Bar> = hist.data
        .iter()
        .map(|(x, y)| Bar::new(*x, *y))
        .collect();

    let chart = BarChart::new(bars)
        .width(hist.width);

    Plot::new("histogram")
        .height(300.0)
        .show(ui, |plot_ui| {
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
