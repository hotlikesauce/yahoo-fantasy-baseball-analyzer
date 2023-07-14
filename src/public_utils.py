import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
import time, datetime, os
import warnings


warnings.simplefilter(action='ignore', category=FutureWarning)

class LiveStandings:
    def __init__(self,YAHOO_LEAGUE_ID):
        self.YAHOO_LEAGUE_ID = YAHOO_LEAGUE_ID
        self.df_liveStandings = None

    def set_this_week(self):
        my_date = datetime.date.today()
        year, week_num, day_of_week = my_date.isocalendar()
        
        # Adjust to match All-Star break. 13 for pre-All-Star.
        # The if statement below handles the 2-week ASG Break, which happens on week 30 of the calendar year.
        if week_num < 30:
            return week_num - 13
        else:
            return week_num - 14


    def get_current_matchups(self,YAHOO_LEAGUE_ID):
        league_size = league_size()
        YAHOO_LEAGUE_URL=('https://baseball.fantasysports.yahoo.com/b1/'+YAHOO_LEAGUE_ID)
        df_teamRecords = pd.DataFrame(columns = ['Team','Team_Wins','Team_Loss','Team_Draw','Record'])
        df_weeklyMatchups = pd.DataFrame(columns = ['Team','Record'])

        for matchup in range(1,(league_size+1)):
            #To prevent DDOS, Yahoo limits your URL requests over a set amount of time. Sleep timer to hlep space our requests
            time.sleep(1)
            df_currentMatchup = pd.DataFrame(columns = ['Team','Score'])
            
            thisWeek = self.set_this_week()

            #This is the correct URL, it gets team totals as opposed to the standard matchup page which has weird embedded tables
            source = uReq(YAHOO_LEAGUE_URL+'/matchup?date=totals&week='+str(thisWeek)+'&mid1='+str(matchup)).read()
            #print(YAHOO_LEAGUE_URL+'/matchup?date=totals&week='+str(thisWeek)+'&mid1='+str(matchup))
            soup = bs.BeautifulSoup(source,'lxml')

            table = soup.find_all('table')
            df_currentMatchup = pd.read_html(str(table))[1]
            df_currentMatchup.columns = df_currentMatchup.columns[:-1].tolist() + ['Score']

            #print(df_currentMatchup)
            
            df_currentMatchup=df_currentMatchup[['Team','Score']]

            df_teamRecords['Team'] = df_currentMatchup['Team']
            df_teamRecords['Team_Wins'] = df_currentMatchup['Score'].iloc[0]
            df_teamRecords['Team_Loss'] = df_currentMatchup['Score'].iloc[1]
            if df_teamRecords['Team_Wins'].iloc[0] + df_teamRecords['Team_Loss'].iloc[0] == league_size:
                df_teamRecords['Team_Draw'] = 0
                df_teamRecords['Record'] = list(zip(df_teamRecords.Team_Wins,df_teamRecords.Team_Draw,df_teamRecords.Team_Loss))
            else:
                df_teamRecords['Team_Draw'] = league_size - (df_teamRecords['Team_Wins'].iloc[0] + df_teamRecords['Team_Loss'].iloc[0])
                df_teamRecords['Record'] = list(zip(df_teamRecords.Team_Wins,df_teamRecords.Team_Draw,df_teamRecords.Team_Loss))
            
            # print(df_teamRecords[['Team','Record']].loc[0])

            df_weeklyMatchups = df_weeklyMatchups.append(df_teamRecords.loc[0], True)

        #print(df_weeklyMatchups[['Team','Record']])
        
        return df_weeklyMatchups

    def get_standings(self,df_currentMatchup,YAHOO_LEAGUE_ID):
        YAHOO_LEAGUE_URL=('https://baseball.fantasysports.yahoo.com/b1/'+YAHOO_LEAGUE_ID)
        source = uReq(YAHOO_LEAGUE_URL).read()
        soup = bs.BeautifulSoup(source,'lxml')

        table = soup.find_all('table')
        df_seasonRecords = pd.read_html(str(table))[0]

        df_seasonRecords.columns = df_seasonRecords.columns.str.replace('[-]', '')

        new = df_seasonRecords['WLT'].str.split("-", n = 2, expand = True)
        new = new.astype(int)
        # making separate first name column from new data frame
        df_seasonRecords["WLT_Win"]= new[0]
        df_seasonRecords["WLT_Loss"]= new[1]
        df_seasonRecords["WLT_Draw"]= new[2]
        df_seasonRecords['Raw_Score_Static'] = (df_seasonRecords['WLT_Win'] + (df_seasonRecords['WLT_Draw']*.5))

        #calculate live standings from season records and live records
        df_liveStandings = df_seasonRecords.merge(df_currentMatchup, on='Team')
        df_liveStandings['Live_Wins'] = df_liveStandings['WLT_Win'] + df_liveStandings['Team_Wins']
        df_liveStandings['Live_Loss'] = df_liveStandings['WLT_Loss'] + df_liveStandings['Team_Loss']
        df_liveStandings['Live_Draw'] = df_liveStandings['WLT_Draw'] + df_liveStandings['Team_Draw']
        df_liveStandings['Raw_Score'] = (df_liveStandings['Live_Wins'] + (df_liveStandings['Live_Draw']*.5))
        df_liveStandings['Games_Back'] = df_liveStandings['Raw_Score'].max() - df_liveStandings['Raw_Score']
        df_liveStandings['Pct'] = (df_liveStandings['Live_Wins'] + (df_liveStandings['Live_Draw']*.5))/(df_liveStandings['Live_Wins'] + df_liveStandings['Live_Draw'] + df_liveStandings['Live_Loss'])
        df_liveStandings['Current Matchup'] = df_liveStandings['Team_Wins'].astype(int).astype(str) + '-' + df_liveStandings['Team_Loss'].astype(int).astype(str) + '-' + df_liveStandings['Team_Draw'].astype(int).astype(str)
        df_liveStandings = df_liveStandings.sort_values(by=['Pct'],ascending=False,ignore_index=True)
        try:        
            df_liveStandings['Rank'] = df_liveStandings['Rank'].str.replace('*','').astype(int)
        except AttributeError:
            pass
        return df_liveStandings
        

    def fetch_live_standings(self,YAHOO_LEAGUE_ID):
        try:
            print("ETA 20 Seconds Please Hold")
            df_currentMatchup = self.get_current_matchups(YAHOO_LEAGUE_ID)
            self.df_liveStandings = self.get_standings(df_currentMatchup,self.YAHOO_LEAGUE_ID)
            return self.df_liveStandings

        except Exception as e:
            filename = os.path.basename(__file__)
            error_message = str(e)
            print(error_message)

def get_live_standings(league_id):
    standings = LiveStandings(str(league_id))
    df = standings.fetch_live_standings(str(league_id))
    return df