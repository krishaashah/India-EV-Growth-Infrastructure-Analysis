import pandas as pd

def clean_vehicle_file(file_path, year, value_col_name):
    df = pd.read_excel(file_path, header=1)

    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )

    if "state" not in df.columns:
        df = df.rename(columns={df.columns[1]: "state"})

    df = df[df["state"].notna()]
    df["state"] = df["state"].astype(str).str.strip()
    df = df[~df["state"].str.contains("total", case=False, na=False)]

    for col in df.columns:
        if col != "state":
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("nan", "", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "total" in df.columns:
        df[value_col_name] = df["total"]
    else:
        raise ValueError(f"Total column not found in dataset: {file_path}")

    df = df[["state", value_col_name]].copy()
    df["year"] = year
    df = df[["state", "year", value_col_name]]

    return df

#FOUR WHEELER FILES


four_wheeler_files = [
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\four_wheeler\Four_wheeler_2021.xlsx", 2021),
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\four_wheeler\Four_wheeler_2022.xlsx", 2022),
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\four_wheeler\Four_wheeler_2023.xlsx", 2023),
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\four_wheeler\Four_wheeler_2024.xlsx", 2024),
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\four_wheeler\Four_wheeler_2025.xlsx", 2025),
]

four_wheeler_data = []

for file_path, year in four_wheeler_files:
    df = clean_vehicle_file(file_path, year, "total_ev_4")
    four_wheeler_data.append(df)

df_4 = pd.concat(four_wheeler_data, ignore_index=True)

print("4-wheeler dataset:")
print(df_4.head())

#THREE WHEELER FILES


three_wheeler_files = [
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\three_wheeler\Three_wheeler_2021.xlsx", 2021),
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\three_wheeler\Three_wheeler_2022.xlsx", 2022),
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\three_wheeler\Three_wheeler_2023.xlsx", 2023),
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\three_wheeler\Three_wheeler_2024.xlsx", 2024),
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\three_wheeler\Three_wheeler_2025.xlsx", 2025),
]

three_wheeler_data = []

for file_path, year in three_wheeler_files:
    df = clean_vehicle_file(file_path, year, "total_ev_3")
    three_wheeler_data.append(df)

df_3 = pd.concat(three_wheeler_data, ignore_index=True)

print("3-wheeler dataset:")
print(df_3.head())

#TWO WHEELER DATA

two_wheeler_files = [
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\two_wheeler\Two_wheeler_2021.xlsx", 2021),
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\two_wheeler\Two_wheeler_2022.xlsx", 2022),
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\two_wheeler\Two_wheeler_2023.xlsx", 2023),
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\two_wheeler\Two_wheeler_2024.xlsx", 2024),
    (r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\vehicle vise data\two_wheeler\Two_wheeler_2025.xlsx", 2025),
]

two_wheeler_data = []

for file_path, year in two_wheeler_files:
    df = clean_vehicle_file(file_path, year, "total_ev_2")
    two_wheeler_data.append(df)

df_2 = pd.concat(two_wheeler_data, ignore_index=True)

print("2-wheeler dataset:")
print(df_2.head())


#MERGE AND CREATING MAIN DATASET
main_df = df_4.merge(df_3, on=["state", "year"], how="outer")
main_df = main_df.merge(df_2, on=["state", "year"], how="outer")

print("Merged dataset:")
print(main_df.head())


main_df["total_ev_4"] = main_df["total_ev_4"].fillna(0).astype(int)
main_df["total_ev_3"] = main_df["total_ev_3"].fillna(0).astype(int)
main_df["total_ev_2"] = main_df["total_ev_2"].fillna(0).astype(int)

main_df = main_df[["state", "year", "total_ev_4", "total_ev_3", "total_ev_2"]]

main_df = main_df.sort_values(by=["state", "year"]).reset_index(drop=True)

print("Final dataset:")
print(main_df.head(10))

output_path = r"C:\Users\dhariya\Desktop\EDA_Krisha_Project\EV\final_ev_dataset.csv"
main_df.to_csv(output_path, index=False)

print("Saved successfully at:")
print(output_path)