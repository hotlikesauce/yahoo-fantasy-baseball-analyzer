from live_standings.get_power_rankings import main as get_power_rankings_main
from weekly_stats.get_all_play import main as get_all_play_main
from weekly_stats.get_season_trend_power_ranks import main as get_season_trend_power_ranks_main
from weekly_stats.get_season_trend_standings import main as get_season_trend_standings_main


# def main():
#     functions = [
#         get_power_rankings_main#,
#         #get_all_play_main,
#         #get_season_trend_power_ranks_main,
#         #get_season_trend_standings_main
#     ]

#     for func in functions:
#         try:
#             func()
#         except Exception as e:
#             print(f"Error in {func.__name__}: {str(e)}")

# if __name__ == '__main__':
#     main()