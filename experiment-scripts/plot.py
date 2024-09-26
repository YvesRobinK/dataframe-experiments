from absl import app
from absl import flags

import matplotlib.cm as cm
from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib.patches as mpatches
import matplotlib.lines as mlines

import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = None


SF1 = 53446198

FLAGS = flags.FLAGS
#'/home/yves/Desktop/MasterThesis/master_thesis/snowflake_scipts/rumblebd-snowflake-scripts-master/workloads/ssb/experiment-scripts/experiments/snowflake_ssb_run_2024-03-31_20_19_13_857/summary.log.csv'
flags.DEFINE_list('summary_paths', None, 'The path to your summary file.')
flags.DEFINE_enum('plot_type', 'execution-time', ['execution-time', 
  'compilation-time', 'bytes-scanned', 'credits-used', 'total-time', 
  'generation-time', 'sweep', "total-time-full", "bytes-local-spill",
  "bytes-remote-spill", "bytes-sent-over-network", "partition-scan-fraction",
  "ssb-sweep"], 'The type of the generated plot.')
flags.DEFINE_string('output_path', '.', 'The location where the plot will be dumped')

PLOT_PARAMETERS = {
  "execution-time": {
    "y_col_name": "EXECUTION_TIME",
    "x_label_rotation": 0,
    "y_axis_log": True,
    "legend_params": {
      "title": "",    
    },
    "pd_params": {
      "xlabel": "Query",
      "ylabel": "Query Runtime [s]",
      "capsize": 3, 
      "edgecolor": 'black', 
      "linewidth": 2,
      "figsize": (8, 4),
      "zorder": 3
    },
    "show_errbar": True,
    "save_parameters": {
      "bbox_inches": 'tight', 
      "pad_inches": 0,
    },
    "annotate_bar": True,
    "bar_annotation_params": {
      "ha": "center", 
      "va": "bottom", 
      "fontsize": "small", 
      "color": "white",
      "rotation": "vertical"
    },
    "bar_annotation_metaparams": {
      "division_fraction": 10000,
      "rounding_digit": 2
    }
  }
}

PLOT_PARAMETERS["compilation-time"] = PLOT_PARAMETERS["execution-time"] | {
  "y_col_name": "COMPILATION_TIME",
  "pd_params": PLOT_PARAMETERS["execution-time"]["pd_params"] | {
      "xlabel": "Query",
      "ylabel": "Snowflake Query Compilation Time [s]",
    },
  "bar_annotation_metaparams": PLOT_PARAMETERS["execution-time"]["bar_annotation_metaparams"] | {
    "division_fraction": 2500,
    "fontsize": "xxx-small",
  },
}

PLOT_PARAMETERS["total-time-full"] = PLOT_PARAMETERS["execution-time"] | {
  "y_col_name": "TOTAL_ELAPSED_TIME",
  "pd_params": PLOT_PARAMETERS["execution-time"]["pd_params"] | {
      "xlabel": "Query",
      "ylabel": "Total Time [s]",
    },
  "bar_annotation_metaparams": PLOT_PARAMETERS["execution-time"]["bar_annotation_metaparams"] | {
    "division_fraction": 6500
  },
  "show_errbar": False,
  "annotate_bar": False,
  "x_axis_labels": ["q1.1", "q1.2", "q1.3", "q2.1", "q2.2", "q2.3", "q3.1", "q3.2", "q3.3", "q3.4", "q4.1", "q4.2", "q4.3"],
}

PLOT_PARAMETERS["bytes-scanned"] = PLOT_PARAMETERS["execution-time"] | {
  "y_col_name": "BYTES_SCANNED",
  "pd_params": PLOT_PARAMETERS["execution-time"]["pd_params"] | {
      "ylabel": "Data Scanned [GiB]",
    },
  "show_errbar": False
}

PLOT_PARAMETERS["bytes-local-spill"] = PLOT_PARAMETERS["execution-time"] | {
  "y_col_name": "BYTES_SPILLED_TO_LOCAL_STORAGE",
  "pd_params": PLOT_PARAMETERS["execution-time"]["pd_params"] | {
      "ylabel": "Spilled [GiB]",
    },
  "show_errbar": False
}

PLOT_PARAMETERS["bytes-remote-spill"] = PLOT_PARAMETERS["execution-time"] | {
  "y_col_name": "BYTES_SPILLED_TO_REMOTE_STORAGE",
  "pd_params": PLOT_PARAMETERS["execution-time"]["pd_params"] | {
      "ylabel": "Spilled [GiB]",
    },
  "show_errbar": False
}

PLOT_PARAMETERS["bytes-sent-over-network"] = PLOT_PARAMETERS["execution-time"] | {
  "y_col_name": "BYTES_SENT_OVER_THE_NETWORK",
  "pd_params": PLOT_PARAMETERS["execution-time"]["pd_params"] | {
      "ylabel": "Sent Data [GiB]",
    },
  "show_errbar": False
}


PLOT_PARAMETERS["partition-scan-fraction"] = PLOT_PARAMETERS["execution-time"] | {
  "y_col_name": "PARTITION_SCAN_FRACTION",
  "pd_params": PLOT_PARAMETERS["execution-time"]["pd_params"] | {
      "ylabel": "Paritions Scanned [%]",
    },
  "show_errbar": False
}

PLOT_PARAMETERS["credits-used"] = PLOT_PARAMETERS["execution-time"] | {
  "y_col_name": "CREDITS_USED_CLOUD_SERVICES",
  "pd_params": PLOT_PARAMETERS["execution-time"]["pd_params"] | {
      "ylabel": "Credits Used",
    },
}

PLOT_PARAMETERS["total-time"] = PLOT_PARAMETERS["execution-time"] | {
  "y_col_name": ["EXECUTION_TIME", "COMPILATION_TIME"],
  "pd_params": PLOT_PARAMETERS["execution-time"]["pd_params"] | {
      "ylabel": "Total Time [s]",
    }
}

PLOT_PARAMETERS["generation-time"] = PLOT_PARAMETERS["execution-time"] | {
  "y_col_name": "queryGenerationTimeMean",
  "y_col_err_name": "queryGenerationTimeStd",
  "pd_params": PLOT_PARAMETERS["execution-time"]["pd_params"] | {
      "ylabel": "Query Generation Time [s]",
  },
  "y_axis_log": False,
  "bar_annotation_params": PLOT_PARAMETERS["execution-time"]["bar_annotation_params"] | {
    "rotation": "horizontal",
    "fontsize": "x-small",
  },
  "bar_annotation_metaparams": PLOT_PARAMETERS["execution-time"]["bar_annotation_metaparams"] | {
    "division_fraction": 100,
    "rounding_digit": 2,
  },
  "filters": [lambda x: "rumbledb" in x["experimentID"].lower()]
}

PLOT_PARAMETERS["sweep"] = PLOT_PARAMETERS["execution-time"] | {
  "y_col_name": "TOTAL_ELAPSED_TIME",
  "pd_params": {
      "xlabel": "SSB Scale Factor",
      "ylabel": "Query Runtime [s]",
      "linewidth": 2,
      "figsize": (8, 4),
      "marker": "o", 
    },
  "x_axis_log": True,
  "y_axis_log": True,
  "label_fontsize": 9,
  "x_lim": [1, 1050],
  "y_lim": [0.1, 10],
  "x_axis_positions": [1, 10, 100, 1000],
  # "x_axis_labels": ["SF1", "SF10", "SF100", "SF1000"],
  "x_axis_labels": ["1", "10", "100", "1000"],
}

PLOT_PARAMETERS["ssb-sweep"] = PLOT_PARAMETERS["execution-time"] | {
  "y_col_name": "TOTAL_ELAPSED_TIME",
  "pd_params": {
      "xlabel": "SSB Scale Factor",
      "ylabel": "Query Runtime [s]",
      "linewidth": 2,
      "figsize": (8, 4),
    },
  "x_axis_log": True,
  "y_axis_log": True,
  "label_fontsize": 9,
  "x_lim": [1, 1050],
  "y_lim": [0.1, 10],
  "x_axis_positions": [1, 10, 100, 1000],
  # "x_axis_labels": ["SF1", "SF10", "SF100", "SF1000"],
  "x_axis_labels": ["1", "10", "100", "1000"],
}

x_axis_label_map = {
  "ADL": (16, "1"),
  "ADL_1000": (0, "1/2^16"),
  "ADL_1024000": (10, "1/64"),
  "ADL_128000": (7, "1/512"),
  "ADL_16000": (4, "1/2^12"),
  "ADL_16384000": (14, "1/4"),
  "ADL_2000": (1, "1/2^15"),
  "ADL_2048000": (11, "1/32"),
  "ADL_256000": (8, "1/256"),
  "ADL_32000": (5, "1/2^11"),
  "ADL_32768000": (15, "1/2"),
  "ADL_4000": (2, "1/2^14"),
  "ADL_4096000": (12, "1/16"),
  "ADL_512000": (9, "1/128"),
  "ADL_64000": (6, "1/1024"),
  "ADL_8000": (3, "1/2^13"),
  "ADL_8192000": (13, "1/8"),
  "ADL_SF16": (20, "16"),
  "ADL_SF2": (17, "2"),
  "ADL_SF32": (21, "32"),
  "ADL_SF4": (18, "4"),
  "ADL_SF64": (22, "64"),
  "ADL_SF8": (19, "8")
}


def preprocessDF(df, filters=None):
  def _experiment_rename(x):
    x = x.lower()
    if "snowflake" in x:
      return "Hand-Written SQL"
    if "rumbledb_join_run" in x:
      return "Automatically Generated SQL"
    if "rumbledb_join" in x:
      return "[JOIN-based] Automatically Generated SQL"
    if "rumbledb_indicator" in x:
      return "[FLAG-based] Automatically Generated SQL"
    if "RumbleDBSpark" in x:
      return "RumbleDB+Spark"
    if "AsterixDB" in x:
      return "AsterixDB"
    if "rumbledb" in x:
      return "Automatically Generated SQL"
    return "Unknown"


  def _query_tag_rename(x):
    x = x.lower()
    if "part-partsupp-supplier-lineitem-orders-customer-cnr" in x:
      return "P-PS-S-L-O-C-CNR"
    if "part-partsupp-supplier-lineitem-orders-customer" in x:
      return "P-PS-S-L-O-C"
    if "part-partsupp-supplier-lineitem-orders" in x:
      return "P-PS-S-L-O"
    if "part-partsupp-supplier-lineitem" in x:
      return "P-PS-S-L"
    if "part-partsupp-supplier" in x:
      return "P-PS-S"
    if "part-partsupp" in x:
      return "P-PS"
    if "all" in x:
      return "All"
    
    return x

  # Filter out any potentially useless data
  # if filters is not None:
  #   for f in filters:
  #     df = df[df.apply(f, axis=1)]

  # Preprocess the DF
  #df['experimentID'] = df['experimentID'].map(_experiment_rename)
  df['queryTag'] = df['queryTag'].map(_query_tag_rename)

  df['queryGenerationTimeMean'] /= 1000
  df['queryGenerationTimeStd'] /= 1000
  df['schemaName'] = df['schemaName'].astype(str).str.lower()

  try:  
    df['COMPILATION_TIME'] = df['COMPILATION_TIME'].astype(float)  
    df['EXECUTION_TIME'] = df['EXECUTION_TIME'].astype(float)  
    df['TOTAL_ELAPSED_TIME'] = df['TOTAL_ELAPSED_TIME'].astype(float)  
    df['BYTES_SCANNED'] = df['BYTES_SCANNED'].astype(float) 
    df['BYTES_SPILLED_TO_LOCAL_STORAGE'] = 0.001 + df['BYTES_SPILLED_TO_LOCAL_STORAGE'].astype(float) 
    df['BYTES_SPILLED_TO_REMOTE_STORAGE'] = 0.001 +df['BYTES_SPILLED_TO_REMOTE_STORAGE'].astype(float) 
    df['BYTES_SENT_OVER_THE_NETWORK'] = df['BYTES_SENT_OVER_THE_NETWORK'].astype(float) 

    df['PARTITIONS_SCANNED'] = df['PARTITIONS_SCANNED'].astype(float) 
    df['PARTITIONS_TOTAL'] = df['PARTITIONS_TOTAL'].astype(float) 

    df['COMPILATION_TIME'] /= 1000  
    df['EXECUTION_TIME'] /= 1000  
    df['TOTAL_ELAPSED_TIME'] /= 1000  
    df['BYTES_SCANNED'] /= 2 ** 30 # GiB  
    df['BYTES_SPILLED_TO_LOCAL_STORAGE'] /= 2 ** 30 # GiB  
    df['BYTES_SPILLED_TO_REMOTE_STORAGE'] /= 2 ** 30 # GiB  
    df['BYTES_SENT_OVER_THE_NETWORK'] /= 2 ** 30 # GiB  
    df['PARTITION_SCAN_FRACTION'] = df['PARTITIONS_SCANNED'] / df['PARTITIONS_TOTAL']
  except:
    print("Could not find COMPILATION_TIME, EXECUTION_TIME, BYTES_SCANNED, "
      "BYTES_SPILLED_TO_LOCAL_STORAGE, BYTES_SPILLED_TO_REMOTE_STORAGE, "
      "BYTES_SENT_OVER_THE_NETWORK, PARTITIONS_SCANNED, PARTITIONS_TOTAL; "
      "This probably means you are going over a summary file with no Snowflake "
      "metrics.")

  return df


def barplot(df, plot_name, plot_parameters, basepath="."):
  number_of_experiment_classes = df["experimentID"].nunique()
  df = df[["experimentID", "queryTag", plot_parameters["y_col_name"]] 
    + ([plot_parameters["y_col_err_name"]] if "y_col_err_name" in plot_parameters 
      else [])]

#  if "y_col_err_name" not in plot_parameters:
#    # This is the case when we need to compute the means and std here
#    df = df.groupby(by=["experimentID", "queryTag"])
#    means = df.mean(numeric_only=True).add_suffix("_mean")
#    std = df.std(numeric_only=True).add_suffix("_std")
#    df = means.join(std).reset_index()
  
#    vals_to_plot = {
#      "val": plot_parameters["y_col_name"] + "_mean",
#      "err": plot_parameters["y_col_name"] + "_std"
#    }
#  else:
    # This is the case when the mean and std have already been computed
#    vals_to_plot = {
#      "val": plot_parameters["y_col_name"],
#      "err": plot_parameters["y_col_err_name"]
#    }
  df = df.groupby(by=["experimentID", "queryTag"])
  std = df.std(numeric_only=True).add_suffix("")

  df = df.pivot(index="queryTag", columns="experimentID", 
    values=vals_to_plot.values())


  if "show_errbar" not in plot_parameters or not plot_parameters["show_errbar"]:
    ax = df.plot.bar(y=vals_to_plot["val"], **plot_parameters["pd_params"])
  else:
    if "y_col_err_name" in plot_parameters:
      vals_to_plot["err"] = plot_parameters["y_col_err_name"]
    ax = df.plot.bar(y=vals_to_plot["val"], yerr=vals_to_plot["err"], 
      **plot_parameters["pd_params"])

  # ax.set_xticks(plot_parameters["x_axis_positions"])
  ax.set_xticklabels(plot_parameters["x_axis_labels"], ha="right")
  
  # Set hatches
  bars = ax.patches
  patterns = ['/', '\\', '.'][:number_of_experiment_classes]  # if plot_name != "generation-time" else ['/'] 
  hatches = []  # list for hatches in the order of the bars
  for h in patterns:  # loop over patterns to create bar-ordered hatches
    for i in range(int(len(bars) / len(patterns))):
        hatches.append(h)

  i = 0        
  for bar, hatch in zip(bars, hatches): 
    bar.set(hatch=hatch, alpha=0.99)
    i += 1

    if "annotate_bar" in plot_parameters and plot_parameters["annotate_bar"]:
      offset = 0.02 if i != 9 else 0.035
      y_lim = ax.get_ylim()[1]
      y_pos = (y_lim / plot_parameters["bar_annotation_metaparams"]["division_fraction"])  
      plot_parameters_copy = dict(plot_parameters["bar_annotation_params"])
      if bar.get_height() <= 0.15:
        y_pos = bar.get_height() + offset
        plot_parameters_copy["color"] = "black"

      ax.text(
          bar.get_x() + bar.get_width() / 2, y_pos, # bar.get_height(), #  ,
          round(bar.get_height(), plot_parameters["bar_annotation_metaparams"]["rounding_digit"]), 
          **plot_parameters_copy)


  # Set other parameters
  if "legend_params" in plot_parameters:
    ax.legend(**plot_parameters["legend_params"])

  if "x_label_rotation" in plot_parameters:
    ax.set_xticks(ax.get_xticks(), ax.get_xticklabels(), 
      rotation=plot_parameters["x_label_rotation"], ha='center')

  # if "y_axis_log" in plot_parameters and plot_parameters["y_axis_log"]:
  #   ax.set_yscale('log')
  #   ax.get_yaxis().set_major_formatter(FuncFormatter(
  #     lambda x, p: float(x)))

  # Save the figure
  plt.grid(True, zorder=0)
  plt.savefig(f"{basepath}/ssb_{plot_name}.png", **plot_parameters["save_parameters"])
  plt.savefig(f"{basepath}/ssb_{plot_name}.pdf", **plot_parameters["save_parameters"])


def plot_clustered_stacked(dfall, plot_parameters):
  labels = []
  n_df = len(dfall)
  ax = plt.subplot(111)

  for label, df in dfall.items(): # for each data frame
    ax = df.plot(kind="bar",
                  linewidth=0,
                  stacked=True,
                  ax=ax,
                  legend=False,
                  grid=False,
                  figsize=(8, 4))  # make bar plots
    labels.append(label)

  n_col = len(dfall[labels[0]].columns) 
  n_ind = len(dfall[labels[0]].index)

  hatches=["/", "\\"]
  colors= ["green", "blue", "red", "yellow"]
  colors_light= ["limegreen", "royalblue", "orangered", "gold"]
  print("View: >>>>>>>>>>>>>>>>", df.index)
  
  h,l = ax.get_legend_handles_labels() # get the handles we want to modify
  for i in range(0, n_df * n_col, n_col): # len(h) = n_col * n_df
    for j, pa in enumerate(h[i:i+n_col]):
      for idx, rect in enumerate(pa.patches): # for each index
        if n_col > 1: 
          rect.set_x(rect.get_x() + 1 / float(n_df + 1) * i / float(n_col))
          if j == 0:
        	  rect.set(color=colors[i // 2], alpha=0.99, edgecolor='black', linewidth=2)
          elif j ==1:
        	  rect.set(color="silver", alpha=0.99, edgecolor='black', linewidth=2)
          rect.set_width(1 / float(n_df + 1))
        else:
          rect.set_x(rect.get_x() + 1 / float(n_df + 1) * i / float(n_col))
          rect.set(color=colors[i // 2], alpha=0.99, edgecolor='black', linewidth=2)
          rect.set_width(1 / float(n_df + 1))
        #rect.set(hatch=hatches[j  % 2])
        #rect.set(hatch=hatches[i  len(hatches)], alpha=0.99, edgecolor='black', linewidth=2)     
        #rect.set_width(1 / float(n_df + 1))
        # print(j)
        # if "annotate_bar" in plot_parameters and plot_parameters["annotate_bar"]:
        #   ax.text(
        #       rect.get_x() + rect.get_width() / 2, rect.get_height(), 
        #       round(rect.get_height(), 1) if j == -1 else "", ha="center", va="bottom", 
        #       fontsize="xx-small")


  #ax.set_xticks((np.arange(0, 2 * n_ind, 2) + 1 / float(n_df + 1)) / 2.)
  ax.set_xticklabels(df.index, rotation = 0)

  # Update the legend of the plot
  n=[]        
  for i in range(n_df):
    n.append(ax.bar(0, 0, color=colors[i % n_df], alpha=0.99))#, hatch=hatches[i % n_df]))

  l1 = ax.legend(h[:n_col] + [Line2D([0], [0], color='#ffffff')] + n, 
    l[:n_col] + [""] + labels, **plot_parameters["legend_params"])
  ax.set_xlabel("Query")
  ax.set_ylabel("Query Runtime [s]")

  if "y_axis_log" in plot_parameters:
    #ax.set_yscale('log')
    ax.set_yscale('linear')
    ax.get_yaxis().set_major_formatter(FuncFormatter(
      lambda x, p: float(x)))


def stacked_barplot(df, plot_name, plot_parameters, basepath="."):
  # df = df[df.apply(f, axis=1)]

  # Get a DF for each experiment (Snowflake vs. RumbleDB)
  dfs = {}
  for unique_col_val in df['experimentID'].unique():
    temp = df[df['experimentID'] == unique_col_val]
    #temp = temp[["queryTag", "EXECUTION_TIME"]].groupby(by="queryTag")
    temp = temp[["queryTag", "EXECUTION_TIME", "COMPILATION_TIME"]].groupby(by="queryTag")
    dfs[unique_col_val] = temp.mean(numeric_only=True).rename(columns={
      "EXECUTION_TIME": "Execution",
      "COMPILATION_TIME": "Compilation"})
  
  plot_clustered_stacked(dfs, plot_parameters)

  # Print the latex version
  df = df[["experimentID", "queryTag", "EXECUTION_TIME", "COMPILATION_TIME"]]\
    .rename(columns={
      "experimentID": "Mode", 
      "queryTag": "Query", 
      "EXECUTION_TIME": "Execution", 
      "COMPILATION_TIME": "Compilation"})

  df["Total"] = df["Execution"] + df["Compilation"]
  agg_df = df.groupby(by=["Mode", "Query"])
  means = agg_df.mean(numeric_only=True).add_suffix(" Mean [s]")
  stds = agg_df.std(numeric_only=True).add_suffix(" STD [s]")
  agg_df = means.join(stds).reset_index()

  agg_df.loc[agg_df["Mode"] == "Automatically Generated SQL", "Mode"] = "Ours"
  agg_df.loc[agg_df["Mode"] == "Hand-Written SQL", "Mode"] = "Baseline"

  # agg_df.loc[agg_df["Mode"] == "Ours", "Normalized Total"] = agg_df[]

  df = agg_df.pivot(index="Query", columns="Mode", values=[
    "Compilation Mean [s]", "Execution Mean [s]", "Total Mean [s]"])

  #df["Normalized Total"] = df["Total Mean [s]"]["Ours"] / df["Total Mean [s]"]["Baseline"]   
  print(df.to_latex(float_format="%.2f"))
  # Save the figure
  plt.grid(zorder=0)
  plt.savefig(f"{basepath}/{plot_name}.png", **plot_parameters["save_parameters"])
  plt.savefig(f"{basepath}/{plot_name}.pdf", **plot_parameters["save_parameters"])


def lineplots(df, plot_parameters, plot_type, basepath="."):
  if plot_type == "sweep":
    df['schemaName'] = df['schemaName'].apply(lambda x: int(x.split("_")[1][2:]))

  if plot_parameters["label_fontsize"]:
    plt.rcParams['font.size'] = plot_parameters["label_fontsize"]
  
  if plot_type == "sweep":
    for query in df['queryTag'].unique():
      plot_df = df[df['queryTag'] == query]
      plot_df = plot_df[["experimentID", "schemaName",  
            "TOTAL_ELAPSED_TIME"]].rename(columns={
        "experimentID": "Mode", 
        "schemaName": "Scale", 
        "TOTAL_ELAPSED_TIME": "Total Time"})
      plot_df = plot_df.groupby(by=["Mode", "Scale"])

      means = plot_df.mean(numeric_only=True).add_suffix(" Mean [s]")
      stds = plot_df.std(numeric_only=True).add_suffix(" STD [s]")
      plot_df = means.join(stds).reset_index()

      plot_df = plot_df.pivot(index="Scale", columns="Mode", values=[
        "Total Time Mean [s]", "Total Time STD [s]"])

      ax = plot_df.plot.line(y="Total Time Mean [s]", 
        **plot_parameters["pd_params"])

      # Set other parameters
      ax.margins(0)
      if "legend_params" in plot_parameters:
        ax.legend(**plot_parameters["legend_params"])

      if "x_label_rotation" in plot_parameters:
        ax.set_xticks(ax.get_xticks(), ax.get_xticklabels(), 
          rotation=plot_parameters["x_label_rotation"], ha='right')

      if "y_axis_log" in plot_parameters and plot_parameters["y_axis_log"]:
        ax.set_yscale('log')
        ax.get_yaxis().set_major_formatter(FuncFormatter(
          lambda x, p: float(x)))

      if "x_axis_log" in plot_parameters and plot_parameters["x_axis_log"]:
        ax.set_xscale('log')
      ax.set_xticks(plot_parameters["x_axis_positions"][::2])
      ax.set_xticklabels(plot_parameters["x_axis_labels"][::2])

      if "y_lim" in plot_parameters:
        ax.set_ylim(plot_parameters["y_lim"])
      if "x_lim" in plot_parameters:
        ax.set_xlim(plot_parameters["x_lim"])

      # Save the figure
      plt.grid(zorder=0)
      plt.savefig(f"{basepath}/{query}.png", **plot_parameters["save_parameters"])
      plt.savefig(f"{basepath}/{query}.pdf", **plot_parameters["save_parameters"])
  elif plot_type == "ssb-sweep":
    df['schemaName'] = df['schemaName'].apply(lambda x: int(x.split("_")[1][2:]))
    plot_df = df[["experimentID", "schemaName", "queryTag",  
          "TOTAL_ELAPSED_TIME"]].rename(columns={
      "experimentID": "Mode", 
      "schemaName": "Scale", 
      "queryTag": "Query",
      "TOTAL_ELAPSED_TIME": "Total Time"})
    plot_df = plot_df.groupby(by=["Mode", "Scale", "Query"])

    means = plot_df.mean(numeric_only=True).add_suffix(" Mean [s]")
    stds = plot_df.std(numeric_only=True).add_suffix(" STD [s]")
    plot_df = means.join(stds).reset_index()

    plot_df = plot_df.pivot(index="Scale", columns=["Mode", "Query"], 
      values=["Total Time Mean [s]", "Total Time STD [s]"])

    fig, ax = plt.subplots()
    for mode in [("Automatically Generated SQL", ("solid", "d")), 
      ("Hand-Written SQL", ("dashed", "x"))]:
      for col in [('q1.1', ('b',)), ('q2.1', ('orange',)), ('q3.1', ('r',)), 
        ('q4.1', ('g',))]:  # ('P-PS-S', ('m',)), ('P-PS', ('y'))]:
        ax = plot_df["Total Time Mean [s]"][mode[0]][col[0]].plot.line(  # yerr=plot_df["Total Time STD [s]"][mode[0]][col[0]],
          # y="Total Time Mean [s]", 
          **plot_parameters["pd_params"], linestyle=mode[1][0], color=col[1][0], 
          marker=mode[1][1],  markersize=9, ax=ax)

    # all_patch = mpatches.Patch(facecolor='b', label='All') 
    # p_ps_s_l_o_c_patch = mpatches.Patch(facecolor='orange', label='P-PS-S-L-O-C')
    # p_ps_s_l_o_patch = mpatches.Patch(facecolor='r', label='P-PS-S-L-O')
    # p_ps_s_l_patch = mpatches.Patch(facecolor='g', label='P-PS-S-L')
    # p_ps_s_patch = mpatches.Patch(facecolor='m', label='P-PS-S') 
    # p_ps_patch = mpatches.Patch(facecolor='y', label='P-PS')  
    # auto_translated_patch = mpatches.Patch(facecolor='#ffffff', label='Automatically Translated SQL')
    # manual_patch = mpatches.Patch(facecolor='#ffffff', label='Hand-Written SQL')


    q11_patch = mlines.Line2D([], [], color='b', label='Q1.1') 
    q21_patch = mlines.Line2D([], [], color='orange', label='Q2.1')
    q31_patch = mlines.Line2D([], [], color='r', label='Q3.1')
    q41_patch = mlines.Line2D([], [], color='g', label='Q4.1')
    # p_ps_s_patch = mlines.Line2D([], [], color='m', label='P-PS-S') 
    # p_ps_patch = mlines.Line2D([], [], color='y', label='P-PS')  
    auto_translated_patch = mlines.Line2D([], [], color='#aaaaaa', linestyle="solid", markersize=9, marker="d", label='Automatically Translated SQL')
    manual_patch = mlines.Line2D([], [], color='#aaaaaa', linestyle="dashed", markersize=9, marker="x", label='Hand-Written SQL')
    
    # blue_line = mlines.Line2D([], [], color='blue', marker='*',
    #                       markersize=15, label='Blue stars')

    ax.legend(handles=[q11_patch, q21_patch, auto_translated_patch, q31_patch, 
      q41_patch, manual_patch], loc='upper left', ncol=2) 
      # , p_ps_s_l_patch, p_ps_s_patch, p_ps_patch,
      # ] )

    # Set other parameters
    # ax.margins(0)
    # if "legend_params" in plot_parameters:
    #   ax.legend(**plot_parameters["legend_params"])

    if "x_label_rotation" in plot_parameters:
      ax.set_xticks(ax.get_xticks(), ax.get_xticklabels(), 
        rotation=plot_parameters["x_label_rotation"], ha='right')

    if "y_axis_log" in plot_parameters and plot_parameters["y_axis_log"]:
      ax.set_yscale('log')
      ax.get_yaxis().set_major_formatter(FuncFormatter(
        lambda x, p: float(x)))

    if "x_axis_log" in plot_parameters and plot_parameters["x_axis_log"]:
      ax.set_xscale('log')
    ax.set_xticks(plot_parameters["x_axis_positions"])
    ax.set_xticklabels(plot_parameters["x_axis_labels"])

    if "y_lim" in plot_parameters:
      ax.set_ylim(plot_parameters["y_lim"])
    if "x_lim" in plot_parameters:
      ax.set_xlim(plot_parameters["x_lim"])

    # Save the figure
    plt.grid(zorder=0)
    plt.savefig(f"{basepath}/ssb-sweep.png", **plot_parameters["save_parameters"])
    plt.savefig(f"{basepath}/ssb-sweep.pdf", **plot_parameters["save_parameters"])


def main(argv):
  if FLAGS.summary_paths is None:
    print("You need to supply the path ")
  print("FLAGS_SUMMARY_PATHS", FLAGS.summary_paths)
  # Read the DF
  additional_filters = PLOT_PARAMETERS[FLAGS.plot_type]["filters"] if\
    "filters" in PLOT_PARAMETERS[FLAGS.plot_type] else []
  
  df = pd.concat([preprocessDF(pd.read_csv(path, index_col=False),
    additional_filters) for path in FLAGS.summary_paths])


  # Proceed to plotting the data
  if FLAGS.plot_type in ["execution-time", "compilation-time", "bytes-scanned",
    "credits-used", "generation-time", "total-time-full", "bytes-local-spill",
    "bytes-remote-spill", "bytes-sent-over-network", "partition-scan-fraction"]:
    df = df[df['schemaName'] == 'ssb_sf1000']
    barplot(df, FLAGS.plot_type, PLOT_PARAMETERS[FLAGS.plot_type], 
      FLAGS.output_path)
  elif FLAGS.plot_type == "comparison":
    df = df[df['schemaName'] == 'ssb_sf1000']
    comparison_plot(df, FLAGS.plot_type, PLOT_PARAMETERS[FLAGS.plot_type], 
      FLAGS.output_path)
  elif FLAGS.plot_type == "total-time":
    #df = df[df['schemaName'] == 'ssb_sf1000']
    stacked_barplot(df, FLAGS.plot_type, PLOT_PARAMETERS[FLAGS.plot_type], 
      FLAGS.output_path)
  elif FLAGS.plot_type in ["sweep", "ssb-sweep"]:
    lineplots(df, PLOT_PARAMETERS[FLAGS.plot_type], FLAGS.plot_type, 
      FLAGS.output_path)


if __name__ == '__main__':
  app.run(main)
