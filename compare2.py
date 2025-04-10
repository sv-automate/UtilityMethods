import pandas as pd
import json
import re

def load_csv_files(file1_path: str, file2_path: str) -> tuple:
    df_file1 = pd.read_csv(file1_path)
    df_file2 = pd.read_csv(file2_path)
    return df_file1, df_file2

def compare_counts(df_file1: pd.DataFrame, df_file2: pd.DataFrame) -> dict:
    return {
        "file1_total_records": len(df_file1),
        "file2_total_records": len(df_file2),
        "record_count_difference": abs(len(df_file1) - len(df_file2))
    }

def compare_column_values(df_file1: pd.DataFrame, df_file2: pd.DataFrame, skip_columns=None, primary_column=None):
    if skip_columns is None:
        skip_columns = []

    comparison_result = {}
    columns_to_check = [col for col in df_file1.columns if col not in skip_columns]

    for col in columns_to_check:
        file1_vals = df_file1[col].fillna("__EMPTY__")
        file2_vals = df_file2[col].fillna("__EMPTY__")

        file1_counts = file1_vals.value_counts().to_dict()
        file2_counts = file2_vals.value_counts().to_dict()

        file1_unique_vals = set(file1_vals)
        file2_unique_vals = set(file2_vals)

        common_vals = file1_unique_vals & file2_unique_vals
        file1_only_vals = file1_unique_vals - file2_unique_vals
        file2_only_vals = file2_unique_vals - file1_unique_vals

        def sort_safe(vals):
            return sorted(vals, key=lambda x: str(x))

        common_value_counts = {
            val: {
                "file1_count": int(file1_counts.get(val, 0)),
                "file2_count": int(file2_counts.get(val, 0))
            }
            for val in sort_safe(common_vals)
        }

        file1_only_info = {}
        for val in file1_only_vals:
            matching_rows = df_file1[df_file1[col].fillna("__EMPTY__") == val]
            ids = matching_rows[primary_column].fillna("__EMPTY__").tolist() if primary_column else []
            file1_only_info[val] = {
                "count": int(file1_counts[val]),
                "ids": ids
            }

        file2_only_info = {}
        for val in file2_only_vals:
            matching_rows = df_file2[df_file2[col].fillna("__EMPTY__") == val]
            ids = matching_rows[primary_column].fillna("__EMPTY__").tolist() if primary_column else []
            file2_only_info[val] = {
                "count": int(file2_counts[val]),
                "ids": ids
            }

        comparison_result[col] = {
            "summary": {
                "file1_unique_count": len(file1_unique_vals),
                "file2_unique_count": len(file2_unique_vals),
                "common_value_count": len(common_vals),
                "file1_duplicate_value_count": sum(v for v in file1_counts.values() if v > 1),
                "file2_duplicate_value_count": sum(v for v in file2_counts.values() if v > 1),
            },
            "details": {
                "common_values": common_value_counts,
                "file1_only_values": file1_only_info,
                "file2_only_values": file2_only_info
            }
        }

    return comparison_result

def sanitize_sheet_name(name: str, suffix: str = "") -> str:
    name = re.sub(r"[\\/*?:[\]]", "_", name)
    base = name[:31 - len(suffix)]
    return f"{base}{suffix}"

def save_results_to_json(result: dict, output_path: str):
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=4)

def save_results_to_excel(result: dict, output_path: str):
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        pd.DataFrame([result["record_counts"]]).to_excel(writer, sheet_name="Record_Counts", index=False)

        for col, data in result["column_comparisons"].items():
            summary_df = pd.DataFrame([data["summary"]])
            summary_df.to_excel(writer, sheet_name=sanitize_sheet_name(col, "_Summary"), index=False)

            common_df = pd.DataFrame.from_dict(data["details"]["common_values"], orient='index').reset_index()
            if not common_df.empty:
                common_df.columns = [col, "file1_count", "file2_count"]
            common_df.to_excel(writer, sheet_name=sanitize_sheet_name(col, "_Common"), index=False)

            file1_only_df = pd.DataFrame.from_dict(data["details"]["file1_only_values"], orient='index').reset_index()
            if not file1_only_df.empty:
                file1_only_df.columns = [col, "count", "ids"]
            file1_only_df.to_excel(writer, sheet_name=sanitize_sheet_name(col, "_File1Only"), index=False)

            file2_only_df = pd.DataFrame.from_dict(data["details"]["file2_only_values"], orient='index').reset_index()
            if not file2_only_df.empty:
                file2_only_df.columns = [col, "count", "ids"]
            file2_only_df.to_excel(writer, sheet_name=sanitize_sheet_name(col, "_File2Only"), index=False)

if __name__ == "__main__":
    file1_path = "file1.csv"
    file2_path = "file2.csv"
    skip_columns = ["timestamp", "updated_at"]
    primary_column = "id"

    df_file1, df_file2 = load_csv_files(file1_path, file2_path)
    counts_comparison = compare_counts(df_file1, df_file2)
    column_comparison = compare_column_values(df_file1, df_file2, skip_columns, primary_column)

    final_result = {
        "record_counts": counts_comparison,
        "column_comparisons": column_comparison
    }

    json_output = "comparison_results.json"
    excel_output = "comparison_results.xlsx"

    save_results_to_json(final_result, json_output)
    save_results_to_excel(final_result, excel_output)

    print(f"✅ Comparison complete.\n📄 JSON saved to {json_output}\n📊 Excel saved to {excel_output}")
