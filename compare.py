import pandas as pd
import json

def load_csvs(file1_path: str, file2_path: str):
    df_file1 = pd.read_csv(file1_path)
    df_file2 = pd.read_csv(file2_path)
    return df_file1, df_file2

def get_count_difference(df_file1: pd.DataFrame, df_file2: pd.DataFrame):
    return {
        "file1_count": len(df_file1),
        "file2_count": len(df_file2),
        "difference": abs(len(df_file1) - len(df_file2))
    }

def compare_column_values(df_file1: pd.DataFrame, df_file2: pd.DataFrame):
    comparison_result = {}
    for col in df_file1.columns:
        file1_vals = set(df_file1[col].dropna().unique())
        file2_vals = set(df_file2[col].dropna().unique())
        common_vals = file1_vals & file2_vals
        file1_only_vals = file1_vals - file2_vals
        file2_only_vals = file2_vals - file1_vals

        comparison_result[col] = {
            "summary": {
                "file1_unique_count": len(file1_vals),
                "file2_unique_count": len(file2_vals),
                "common_value_count": len(common_vals),
                "difference_count": len(file1_only_vals) + len(file2_only_vals)
            },
            "details": {
                "common_values": sorted(list(common_vals)),
                "file1_only_values": sorted(list(file1_only_vals)),
                "file2_only_values": sorted(list(file2_only_vals))
            }
        }
    return comparison_result

def find_missing_rows(df_file1: pd.DataFrame, df_file2: pd.DataFrame):
    merged = df_file1.merge(df_file2, how='outer', indicator=True)
    missing = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    return missing

def save_results_as_json(count_diff, column_diff, missing_rows, filename="comparison_results.json"):
    output = {
        "count_difference": count_diff,
        "column_value_comparison": column_diff,
        "missing_rows_in_file2": missing_rows.to_dict(orient="records")
    }
    with open(filename, "w") as f:
        json.dump(output, f, indent=4)

def save_results_as_excel(column_diff, missing_rows, filename="comparison_results.xlsx"):
    with pd.ExcelWriter(filename) as writer:
        for col, data in column_diff.items():
            summary_df = pd.DataFrame([data["summary"]])
            details_df = pd.DataFrame({
                "common_values": pd.Series(data["details"]["common_values"]),
                "file1_only_values": pd.Series(data["details"]["file1_only_values"]),
                "file2_only_values": pd.Series(data["details"]["file2_only_values"])
            })
            summary_df.to_excel(writer, sheet_name=f"{col}_summary", index=False)
            details_df.to_excel(writer, sheet_name=f"{col}_details", index=False)

        missing_rows.to_excel(writer, sheet_name="missing_in_file2", index=False)

def run_comparison(file1_path: str, file2_path: str):
    df_file1, df_file2 = load_csvs(file1_path, file2_path)

    count_diff = get_count_difference(df_file1, df_file2)
    column_diff = compare_column_values(df_file1, df_file2)
    missing_rows = find_missing_rows(df_file1, df_file2)

    # Save outputs
    save_results_as_json(count_diff, column_diff, missing_rows)
    save_results_as_excel(column_diff, missing_rows)

    print("✅ Comparison complete. Results saved to:")
    print(" - comparison_results.json")
    print(" - comparison_results.xlsx")
