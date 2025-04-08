import pandas as pd
import numpy as np
import json

# === Step 1: Load CSV Files ===
def load_csvs(file1_path: str, file2_path: str):
    df_file1 = pd.read_csv(file1_path)
    df_file2 = pd.read_csv(file2_path)
    return df_file1, df_file2

# === Step 2: Count Difference ===
def get_count_difference(df_file1: pd.DataFrame, df_file2: pd.DataFrame):
    return {
        "file1_count": int(len(df_file1)),
        "file2_count": int(len(df_file2)),
        "difference": int(abs(len(df_file1) - len(df_file2)))
    }

# === Step 3: Compare Column Values ===
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
                "file1_unique_count": int(len(file1_vals)),
                "file2_unique_count": int(len(file2_vals)),
                "common_value_count": int(len(common_vals)),
                "difference_count": int(len(file1_only_vals) + len(file2_only_vals))
            },
            "details": {
                "common_values": sorted(map(str, common_vals)),
                "file1_only_values": sorted(map(str, file1_only_vals)),
                "file2_only_values": sorted(map(str, file2_only_vals))
            }
        }
    return comparison_result

# === Step 4: Get Missing Rows in File2 ===
def find_missing_rows(df_file1: pd.DataFrame, df_file2: pd.DataFrame):
    merged = df_file1.merge(df_file2.drop_duplicates(), how='outer', indicator=True)
    missing = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    return missing

# === Step 5: Convert Types for JSON ===
def convert_types(obj):
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    elif pd.isna(obj):
        return None
    return obj

def convert_all(obj):
    if isinstance(obj, dict):
        return {k: convert_all(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_all(item) for item in obj]
    else:
        return convert_types(obj)

# === Step 6: Save Results as JSON ===
def save_results_as_json(count_diff, column_diff, missing_rows, filename="comparison_results.json"):
    output = {
        "count_difference": count_diff,
        "column_value_comparison": column_diff,
        "missing_rows_in_file2": missing_rows.to_dict(orient="records")
    }
    with open(filename, "w") as f:
        json.dump(convert_all(output), f, indent=4)

# === Step 7: Save Results as Excel ===
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

        # Add sheet for missing rows
        if not missing_rows.empty:
            missing_rows.to_excel(writer, sheet_name="missing_in_file2", index=False)

# === Step 8: Main Function to Run All Steps ===
def run_comparison(file1_path: str, file2_path: str):
    df_file1, df_file2 = load_csvs(file1_path, file2_path)

    count_diff = get_count_difference(df_file1, df_file2)
    column_diff = compare_column_values(df_file1, df_file2)
    missing_rows = find_missing_rows(df_file1, df_file2)

    save_results_as_json(count_diff, column_diff, missing_rows)
    save_results_as_excel(column_diff, missing_rows)

    print("✅ Comparison complete.")
    print("📁 JSON saved as: comparison_results.json")
    print("📁 Excel saved as: comparison_results.xlsx")

# === Optional CLI Runner ===
if __name__ == "__main__":
    run_comparison("file1.csv", "file2.csv")
