import os

import pandas as pd
import numpy as np

from scipy.stats import zscore
from sklearn.ensemble import IsolationForest

from ..data_utils import load_data, get_numeric_columns

class DataOutlier(object):
    def __init__(self, file_path : str = "", df: pd.DataFrame = None, output_dir: str = "outlier_analysis") -> None:
        self.file_path = file_path if file_path is not None else "DataFrame filepath not referenced"
        self.df = df if df is not None else load_data(file_path, limit)
        self._output_dir = output_dir

        os.makedirs(self._output_dir, exist_ok=True)

    @property
    def output_dir(self) -> str:
        return self._output_dir

    @output_dir.setter
    def output_dir(self, new_dir: str) -> None:
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        self._output_dir = new_dir

    @staticmethod
    def is_numeric_column(df: pd.DataFrame, col: str) -> bool:
        return pd.api.types.is_numeric_dtype(df[col])
    
    def _remove_outliers(self, col: str) -> None:
        self.df = self.df[self.df[f'outlier_tukey_{col}'] == 0]

    def _impute_mean_outliers(self, col: str) -> None:
        mean_value = self.df[col].mean()
        self.df.loc[self.df[f'outlier_tukey_{col}'] == 1, col] = mean_value

    def _impute_median_outliers(self, col: str) -> None:
        median_value = self.df[col].median()
        self.df.loc[self.df[f'outlier_tukey_{col}'] == 1, col] = median_value

    def _impute_value_outliers(self, col: str, value: float) -> None:
        self.df.loc[self.df[f'outlier_tukey_{col}'] == 1, col] = value

    def _impute_outliers(self, col: str, method: str, value: float = None) -> None:
        methods_of_the_function = {
            "mean": lambda: self._impute_mean_outliers,
            "median": lambda: self._impute_median_outliers,
            "value": lambda: self._impute_value_outliers
        }

        try :
            methods_of_the_function[method](col, value)
        except KeyError:
            raise ValueError(f"Unknown imputation method: {method}")

    def detect_outliers_tukey(self, col: str) -> None:
        q1, q3 = self.df[col].quantile([0.25, 0.75])
        iqr = q3 - q1
        lower_bound, upper_bound = q1 - 1.5 * iqr, q3 + 1.5 * iqr

        self.df[f'outlier_tukey_{col}'] = ((self.df[col] < lower_bound) | (self.df[col] > upper_bound)).astype(int)

    def detect_outliers_zscore(self, col: str, threshold: float = 3.0) -> None:
        self.df[f'outlier_zscore_{col}'] = (np.abs(zscore(self.df[col])) > threshold).astype(int)

class OutlierDetection(DataOutlier):
    __init__ = DataOutlier.__init__

    def detect_outliers_isolation_forest(self, col: str) -> None:
        model = IsolationForest(contamination=0.05, random_state=42)

        self.df[f'outlier_iforest_{col}'] = model.fit_predict(self.df[[col]])
        self.df[f'outlier_iforest_{col}'] = (self.df[f'outlier_iforest_{col}'] == -1).astype(int)

    def summarize_outliers(self, col: str) -> None:
        summary = {
            "Tukey": self.df[f'outlier_tukey_{col}'].sum(),
            "Z-Score": self.df[f'outlier_zscore_{col}'].sum(),
            "Isolation Forest": self.df[f'outlier_iforest_{col}'].sum(),
        }

        print(f"Outlier summary for column '{col}': {summary}")

    def save_cleaned_data(self, output_file: str = "cleaned_data.csv") -> None:
        output_path = os.path.join(self._output_dir, output_file)
        self.df.to_csv(output_path, index=False)
        print(f"Cleaned data saved to: {output_path}")

    def handle_outliers(self, col: str, strategy: str = "remove", value: float = None) -> None:
        methods_of_the_function = {
            "remove": self._remove_outliers,
            "impute_mean": lambda col: self._impute_outliers(col, "mean"),
            "impute_median": lambda col: self._impute_outliers(col, "median"),
            "impute_value": lambda col: self._impute_outliers(col, "value", value)
        }

        if strategy in methods_of_the_function:
            methods_of_the_function[strategy](col)
        else:
            raise ValueError(f"Unknown strategy: {strategy}")

    def run_outlier_analysis(self, strategy: str = "remove", value: float = 2, numerical_cols: list = None) -> None:
        print("Starting outlier analysis...\n")

        numerical_cols = numerical_cols or get_numeric_columns(self.df)
        
        for col in filter(lambda c: self.is_numeric_column(self.df, c), numerical_cols):
            print(f"Analyzing column: {col}")
            
            for method in [self.detect_outliers_tukey, self.detect_outliers_zscore, self.detect_outliers_isolation_forest]:
                method(col)

            self.summarize_outliers(col)
            self.handle_outliers(col, strategy, value)
            
        self.save_cleaned_data()
        print("\nAnalysis complete. Results saved in:", self._output_dir)