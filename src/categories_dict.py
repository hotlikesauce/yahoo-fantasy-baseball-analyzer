batting_categories = {"Games Played":"GP",
"Games Started":"GS_BAT",
"At Bats":"AB",
"Runs":"R",
"Hits":"H",
"Singles":"1B",
"Doubles":"2B",
"Triples":"3B",
"Home Runs":"HR",
"Runs Batted In":"RBI",
"Sacrifice Hits":"SH",
"Sacrifice Flys":"SF",
"Stolen Bases":"SB",
"Caught Stealing":"CS",
"Walks":"BB",
"Intentional Walks":"IBB",
"Hit By Pitch":"HBP",
"Strikeouts":"K_BAT",
"Ground Into Double Play":"GIDP",
"Total Bases":"TB_Bat",
"Putouts":"PO",
"Assists":"A",
"Errors":"ERR",
"Fielding Percentage":"FIELD",
"Batting Average":"BA",
"On-base Percentage":"OBP",
"Slugging Percentage":"SLG",
"On-base + Slugging Percentage":"OPS",
"Extra Base Hits":"EXBH",
"Net Stolen Bases":"NSB",
"Stolen Base Percentage":"SB_PER",
"Hitting for the Cycle":"CYCLE",
"Plate Appearances":"PA",
"Grand Slam Home Runs":"GSHR",
"Outfield Assists":"OA",
"Double Plays Turned":"DPT",
"Catcher Interference":"CI"}


pitching_categories = {"Pitching Appearances":"PITCH_APP",
"Games Started":"GS_PITCH",
"Innings Pitched":"IP",
"Wins":"W",
"Losses":"L",
"Complete Games":"CG",
"Shutouts":"SO",
"Saves":"SV",
"Outs":"O",
"Hits":"H_PITCH",
"Total Batters Faced":"TBF",
"Runs":"R_PITCH",
"Earned Runs":"ER",
"Home Runs":"HRA",
"Walks":"BBA",
"Intentional Walks":"IBBA",
"Hit Batters":"HB",
"Strikeouts":"K_PITCH",
"Wild Pitches":"WP",
"Balks":"BALK",
"Stolen Bases Allowed":"SBA",
"Batters Grounded Into Double Plays":"GIDPF",
"Save Chances":"SV_CHANCE",
"Holds":"HLD",
"Total Bases Allowed":"TBA",
"Earned Run Average":"ERA",
"(Walks + Hits)/ Innings Pitched":"WHIP",
"Strikeouts per Walk Ratio":"KBB",
"Strikeouts per Nine Innings":"K9",
"Pitch Count":"PC",
"Singles Allowed":"1BA",
"Doubles Allowed":"2BA",
"Triples Allowed":"3BA",
"Relief Wins":"RW",
"Relief Losses":"RL",
"Pickoffs":"POFF",
"Relief Appearances":"RAPP",
"On-base Percentage Against":"OBPA",
"Winning Percentage":"WIN_PER",
"Hits Per Nine Innings":"H9",
"Walks Per Nine Innings":"B9",
"No Hitters":"NH",
"Perfect Games":"PG",
"Save Percentage":"SV_PER",
"Inherited Runners Scored":"IR_SCORE",
"Quality Starts":"QS",
"Blown Saves":"BS",
"Net Saves":"NSV",
"Saves + Holds":"SVH",
"Net Saves and Holds":"NSVH",
"Net Wins":"NW"}


Low_Categories = ["CS","ERR","K_BAT","GIDP","ERA","WHIP","IR_SCORE","BS","B9","H9","OBPA","RL","3BA","2BA","1BA","TBA","BALK","WP","HB","IBBA","BBA","HRA","ER","R_PITCH","H_PITCH","L"]

Low_Categories_Stats = ["CS_Stats","ERR_Stats","K_BAT_Stats","GIDP_Stats","ERA_Stats","WHIP_Stats","IR_SCORE_Stats","BS_Stats","B9_Stats","H9_Stats","OBPA_Stats","RL_Stats","3BA_Stats","2BA_Stats","1BA_Stats","TBA_Stats","BALK_Stats","WP_Stats","HB_Stats","IBBA_Stats","BBA_Stats","HRA_Stats","ER_Stats","R_PITCH_Stats","H_PITCH_Stats","L_Stats"]

Low_Categories_Avg = ["CS_Avg","ERR_Avg","K_BAT_Avg","GIDP_Avg","ERA_Avg","WHIP_Avg","IR_SCORE_Avg","BS_Avg","B9_Avg","H9_Avg","OBPA_Avg","RL_Avg","3BA_Avg","2BA_Avg","1BA_Avg","TBA_Avg","BALK_Avg","WP_Avg","HB_Avg","IBBA_Avg","BBA_Avg","HRA_Avg","ER_Avg","R_PITCH_Avg","H_PITCH_Avg","L_Avg"]


batting_abbreviations = {}

pitching_abbreviations = {'K/9':'K9'
                          ,'HR':'HRA'
                          ,'SV+H':'SVH'
                          ,'HR.1':'HRA'}

Batting_Rank_Stats = ["GP_Rank_Stats","GS_BAT_Rank_Stats","AB_Rank_Stats","R_Rank_Stats","H_Rank_Stats","1B_Rank_Stats","2B_Rank_Stats","3B_Rank_Stats","HR_Rank_Stats","RBI_Rank_Stats","SH_Rank_Stats","SF_Rank_Stats","SB_Rank_Stats","CS_Rank_Stats","BB_Rank_Stats","IBB_Rank_Stats","HBP_Rank_Stats","K_BAT_Rank_Stats","GIDP_Rank_Stats","TB_Bat_Rank_Stats","PO_Rank_Stats","A_Rank_Stats","ERR_Rank_Stats","FIELD_Rank_Stats","BA_Rank_Stats","OBP_Rank_Stats","SLG_Rank_Stats","OPS_Rank_Stats","EXBH_Rank_Stats","NSB_Rank_Stats","SB_PER_Rank_Stats","CYCLE_Rank_Stats","PA_Rank_Stats","GSHR_Rank_Stats","OA_Rank_Stats","DPT_Rank_Stats","CI_Rank_Stats"]

Pitching_Rank_Stats = ["PITCH_APP_Rank_Stats","GS_PITCH_Rank_Stats","IP_Rank_Stats","W_Rank_Stats","L_Rank_Stats","CG_Rank_Stats","SO_Rank_Stats","SV_Rank_Stats","O_Rank_Stats","H_PITCH_Rank_Stats","TBF_Rank_Stats","R_PITCH_Rank_Stats","ER_Rank_Stats","HRA_Rank_Stats","BBA_Rank_Stats","IBBA_Rank_Stats","HB_Rank_Stats","K_PITCH_Rank_Stats","WP_Rank_Stats","BALK_Rank_Stats","SBA_Rank_Stats","GIDPF_Rank_Stats","SV_CHANCE_Rank_Stats","HLD_Rank_Stats","TBA_Rank_Stats","ERA_Rank_Stats","WHIP_Rank_Stats","KBB_Rank_Stats","K9_Rank_Stats","PC_Rank_Stats","1BA_Rank_Stats","2BA_Rank_Stats","3BA_Rank_Stats","RW_Rank_Stats","RL_Rank_Stats","POFF_Rank_Stats","RAPP_Rank_Stats","OBPA_Rank_Stats","WIN_PER_Rank_Stats","H9_Rank_Stats","B9_Rank_Stats","NH_Rank_Stats","PG_Rank_Stats","SV_PER_Rank_Stats","IR_SCORE_Rank_Stats","QS_Rank_Stats","BS_Rank_Stats","NSV_Rank_Stats","SVH_Rank_Stats","NSVH_Rank_Stats","NW_Rank_Stats"]

Batting_Avg = ["GP_Avg","GS_BAT_Avg","AB_Avg","R_Avg","H_Avg","1B_Avg","2B_Avg","3B_Avg","HR_Avg","RBI_Avg","SH_Avg","SF_Avg","SB_Avg","CS_Avg","BB_Avg","IBB_Avg","HBP_Avg","K_BAT_Avg","GIDP_Avg","TB_Bat_Avg","PO_Avg","A_Avg","ERR_Avg","FIELD_Avg","BA_Avg","OBP_Avg","SLG_Avg","OPS_Avg","EXBH_Avg","NSB_Avg","SB_PER_Avg","CYCLE_Avg","PA_Avg","GSHR_Avg","OA_Avg","DPT_Avg","CI_Avg"]

Pitching_Avg = ["PITCH_APP_Avg","GS_PITCH_Avg","IP_Avg","W_Avg","L_Avg","CG_Avg","SO_Avg","SV_Avg","O_Avg","H_PITCH_Avg","TBF_Avg","R_PITCH_Avg","ER_Avg","HRA_Avg","BBA_Avg","IBBA_Avg","HB_Avg","K_PITCH_Avg","WP_Avg","BALK_Avg","SBA_Avg","GIDPF_Avg","SV_CHANCE_Avg","HLD_Avg","TBA_Avg","ERA_Avg","WHIP_Avg","KBB_Avg","K9_Avg","PC_Avg","1BA_Avg","2BA_Avg","3BA_Avg","RW_Avg","RL_Avg","POFF_Avg","RAPP_Avg","OBPA_Avg","WIN_PER_Avg","H9_Avg","B9_Avg","NH_Avg","PG_Avg","SV_PER_Avg","IR_SCORE_Avg","QS_Avg","BS_Avg","NSV_Avg","SVH_Avg","NSVH_Avg","NW_Avg"]

percentage_categories = ['WHIP','ERA','K9','OPS','OBP','AVG','H9','OBPA','OBPA']

all_categories = ["GP","GS_BAT","AB","R","H","1B","2B","3B","HR","RBI","SH","SF","SB","CS","BB","IBB","HBP","K_BAT","GIDP","TB_Bat","PO","A","ERR","FIELD","BA","OBP","SLG","OPS","EXBH","NSB","SB_PER","CYCLE","PA","GSHR","OA","DPT","CI","PITCH_APP","GS_PITCH","IP","W","L","CG","SO","SV","O","H_PITCH","TBF","R_PITCH","ER","HRA","BBA","IBBA","HB","K_PITCH","WP","BALK","SBA","GIDPF","SV_CHANCE","HLD","TBA","ERA","WHIP","KBB","K9","PC","1BA","2BA","3BA","RW","RL","POFF","RAPP","OBPA","WIN_PER","H9","B9","NH","PG","SV_PER","IR_SCORE","QS","BS","NSV","SVH","NSVH","NW"]
