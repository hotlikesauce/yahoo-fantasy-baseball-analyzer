# -*- coding: utf-8 -*-

from get_power_rankings import main as get_power_rankings_main
from get_all_play import main as get_all_play_main
from get_season_trend_power_ranks import main as get_season_trend_power_ranks_main
from get_season_trend_standings import main as get_season_trend_standings_main
from get_weekly_prediction import main as get_weekly_prediction_main
from get_weekly_results import main as get_weekly_results
from get_elo import main as get_elo
from export_csv import main as export_csv


def main():
    functions = [
        get_season_trend_power_ranks_main 
        ,get_power_rankings_main 
        ,get_all_play_main 
        ,get_weekly_results
        ,get_season_trend_standings_main 
        ,get_weekly_prediction_main 
        ,get_elo 
        ,export_csv
    ]

    for func in functions:
        try:
            print(f'Executing {func.__name__}')
            func()
            print(f'Completed {func.__name__}')
            print('--------------------------------------------------------------------------')
        except Exception as e:
            print(f"Error in {func.__name__}: {str(e)}")

if __name__ == '__main__':
    main()