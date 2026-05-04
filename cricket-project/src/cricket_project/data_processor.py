import yaml
import pandas as pd
from typing import Dict, Any, List

class DataProcessor:
    def __init__(self, yaml_data) -> None:
        self.yaml_data: str = yaml_data
        self.data: Dict[str, Any] = yaml.safe_load(yaml_data)
        self.deliveries: List[Dict[str, any]] = self.data['deliveries']
        self.normalised_df: pd.DataFrame = None
        self.deduped_df: pd.DataFrame = None

    def normalise(self) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for delivery in self.deliveries:
            row: Dict[str, Any] = {
                'ball': delivery['ball'],
                'batsman': delivery['batsman'],
                'non_striker': delivery['non_striker'],
                'bowler': delivery['bowler'],
                'runs_total': delivery['runs']['total'],
                'runs_batsman': delivery['runs']['batsman'],
                'runs_extras': delivery['runs']['extras'],
                'is_wicket': 'wicket' in delivery,
                'player_out': delivery.get('wicket', {}).get('player_out', None) if 'wicket' in delivery else None,
                'wicket_type': delivery.get('wicket', {}).get('kind', None) if 'wicket' in delivery else None,
                'wicket_fielders': delivery.get('wicket', {}).get('fielders', None) if 'wicket' in delivery else None,
            }
            rows.append(row)
        self.normalised_df = pd.DataFrame(rows)
        return self.normalised_df

    def dedupe(self) -> pd.DataFrame :
        self.deduped_df: pd.DataFrame = self.normalised_df.drop_duplicates(
            subset=['ball'],
            keep='first'
        )
        return self.deduped_df

    def calculate_total_runs(self):
        if self.deduped_df is None:
            raise ValueError('Must call dedupe() before calculate_total_runs()')

        df_copy: pd.DataFrame = self.deduped_df.copy()
        df_copy['over'] = df_copy['ball'].apply(
            lambda x: int(str(x).split('.')[0])
        )
        runs_per_over: pd.DataFrame = (
            df_copy.groupby('over')['runs_total'].sum().reset_index()
        )
        runs_per_over.columns = ['over', 'total_runs']
        return runs_per_over

    def summarise_batters(self):
        if self.deduped_df is None:
            raise ValueError('Must call dedupe() before summarise_batters()')

        summary = self.deduped_df.groupby('batsman').agg({
            'ball': 'count',
            'runs_total': 'sum',
            'is_wicket': 'sum'
        }).reset_index()

        summary.columns = ['batsman', 'balls_faced', 'total_runs', 'wickets']
        summary['is_out'] = summary['wickets'] > 0

        return summary

    def process_batch(self, output_file='cricket_data_1.csv') -> pd.DataFrame:
        # TODO: Run the pipeline and save to CSV
        self.normalise()
        self.dedupe()

        batter_summary = self.summarise_batters()
        runs_summary = self.calculate_total_runs()
        try:
            runs_summary.to_csv(output_file, index=False)
            print('Success!')
        except:
            print('Something went wrong!')




        