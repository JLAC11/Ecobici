import pandas as pd
import os
from tqdm import tqdm


def join_files(directory):
    """
    Joins all CSV files in the specified directory into a single DataFrame.

    Args:
        directory (str): The path to the directory containing the CSV files.

    Returns:
        pd.DataFrame: A DataFrame containing the combined data from all CSV files.
    """
    all_files = [f for f in os.listdir(directory) if f.endswith(".csv")]
    dfs = {}
    pbar = tqdm(all_files, desc="Joining files", unit="file")
    for file in pbar:
        pbar.set_postfix(file=file)
        file_path = os.path.join(directory, file)
        df = pd.read_csv(
            file_path,
            dtype={
                "Genero_Usuario": "str",
                "Edad_Usuario": "float",
                "Bici": "str",
                "Ciclo_Estacion_Retiro": "str",
                "Ciclo_Estacion_Arribo": "str",
                "Ciclo_EstacionArribo": "str",
                "Hora_Retiro": "str",
                "Hora_Retiro.1": "str",
                "Hora_retiro": "str",
                "Hora_Arribo": "str",
                "Hora_arribo": "str",
                "Hora Arribo": "str",
            },
        )
        pipeline(df)
        dfs[file] = df

    combined_df = pd.concat(list(dfs.values()), ignore_index=True)

    return combined_df


def pipeline(df):
    """Prepares the DataFrame by renaming columns and extracting origin and destination.

    Args:
        df (pd.DataFrame): Ecobici DataFrame to be processed.
        inplace (bool, optional):. Defaults to True.
    """
    df["origen"] = df[
        df.columns[
            df.columns.isin(
                ["Ciclo_Estacion_Arribo", "Ciclo_EstacionArribo", "CE_arribo"]
            )
        ]
    ].bfill(axis=1)
    df["destino"] = df[
        df.columns[df.columns.isin(["Ciclo_Estacion_Retiro", "CE_retiro"])]
    ].bfill(axis=1)
    df["fecha_retiro"] = df[
        df.columns[df.columns.isin(["Fecha_Retiro", "Fecha_retiro"])]
    ].bfill(axis=1)

    df["hora_retiro"] = df[
        df.columns[df.columns.isin(["Hora_Retiro", "Hora_retiro"])]
    ].bfill(axis=1)
    df["fecha_arribo"] = df[
        df.columns[df.columns.isin(["Fecha_Arribo", "Fecha Arribo", "Fecha_arribo"])]
    ].bfill(axis=1)
    # Hora_Retiro.1 is an error in 2021-06-01, it should be Hora_Arribo
    # but it is used in the same way as Hora_Arribo, so we can use it to fill Hora_Arribo
    df["hora_arribo"] = df[
        df.columns[
            df.columns.isin(
                ["Hora_Arribo", "Hora Arribo", "Hora_arribo", "Hora_Retiro.1"]
            )
        ]
    ].bfill(axis=1)
    df["genero"] = df[
        df.columns[df.columns.isin(["Genero_Usuario", "Genero_usuario"])]
    ].bfill(axis=1)
    df["edad"] = df[
        df.columns[df.columns.isin(["Edad_Usuario", "Edad_usuario"])]
    ].bfill(axis=1)
    df["retiro"] = pd.to_datetime(
        df["fecha_retiro"] + " " + df["hora_retiro"],
        format="mixed",
        errors="raise",
    )
    df["arribo"] = pd.to_datetime(
        df["fecha_arribo"] + " " + df["hora_arribo"],
        format="mixed",
        errors="raise",
    )

    df.drop(
        columns=df.columns[
            df.columns.isin(
                [
                    "Ciclo_Estacion_Retiro",
                    "Ciclo_Estacion_Arribo",
                    "Ciclo_EstacionArribo",
                    "CE_arribo",
                    "CE_retiro",
                    "Fecha_Retiro",
                    "Fecha_retiro",
                    "Hora_Retiro",
                    "Hora_Retiro.1",
                    "Hora_retiro",
                    "Fecha_Arribo",
                    "Fecha Arribo",
                    "Fecha_arribo",
                    "Hora_Arribo",
                    "Hora Arribo",
                    "Hora_arribo",
                    "Genero_Usuario",
                    "Genero_usuario",
                    "Edad_Usuario",
                    "Edad_usuario",
                    "fecha_arribo",
                    "hora_arribo",
                    "fecha_retiro",
                    "hora_retiro",
                ]
            )
        ],
        inplace=True,
    )


filepath = "data/historic/"
print("Joining files from directory:", filepath)
data = join_files(filepath)
data.to_parquet("data/ecobici_data.parquet", index=False)
