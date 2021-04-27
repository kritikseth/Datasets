import pandas as pd

class DataFrame:

    def __init__(self, df=None, target:str=None):
        self.__df = df
        if target != None:
            self.y = df[target]
            self.__df.drop([target], axis=1, inplace=True)
    
    def __call__(self):
        return self.__df
    
    @property
    def value(self):
        return self.__df
    
    @value.setter
    def value(self, df):
        self.__df = df
    
    @property
    def shape(self):
        return self.__df.shape
    
    @property
    def columns(self):
        return self.__df.columns
    
    def __col_rename(self, newDf, prefix, on):
        cols = newDf.columns.tolist()
        cols = on + [prefix + col for col in cols if col not in on]
        newDf.columns = cols
        return newDf
    
    def __add(self, newDf, on):
        self.__df = pd.merge(self.__df, newDf, on=on, how='left')
    
    def __split(self, dfCol, playerCol):
        self.__df[playerCol] = self.__df[playerCol].apply(lambda x: x.split(', '))
        ind = dfCol.index(playerCol)
        arr = self.__df.values
        narr = []
        for row in arr:
            for p in row[ind]:
                narr.append(list(row[:ind]) + [p] + row[ind+1:].tolist())
        cols = self.__df.columns.tolist()
        self.__df = pd.DataFrame(narr)
        self.__df.columns = cols

    def add_cols(self, newDf, prefix, on):
        newDf = self.__col_rename(newDf, prefix, on)
        self.__add(newDf, on)

    def split_add(self, newDf, playerCol, prefix, on):
        self.__df = self.__df.reset_index()
        self.__split(self.columns.tolist(), playerCol)
        self.add_cols(newDf, prefix, on)
        df = self.__df
        cols = self.columns
        df = df.groupby(['index'], as_index=False).agg('mean')
        cols = list(set(cols.tolist()) - set(df.columns.tolist())) + ['index']
        self.__df = self.__df[cols]
        self.__df = pd.merge(self.__df, df, on='index', how='right')
        self.__df.drop([playerCol] + ['index'], axis=1, inplace=True)
        self.__df = pd.DataFrame(self.__df)
        self.__df.drop_duplicates(keep='first', inplace=True)

venue_id = {'M.Chinnaswamy Stadium': 0, 'Punjab Cricket Association Stadium': 1, 'Feroz Shah Kotla': 2, 'Wankhede Stadium': 3, 'Eden Gardens': 4, 'Sawai Mansingh Stadium': 5, 'Rajiv Gandhi International Stadium': 6,
            'MA Chidambaram Stadium': 7, 'Dr DY Patil Sports Academy': 8, 'Newlands': 9, "St George's Park": 10, 'Kingsmead': 11, 'SuperSport Park': 12, 'Buffalo Park': 13, 'New Wanderers Stadium': 14,
            'De Beers Diamond Oval': 15, 'OUTsurance Oval': 16, 'Brabourne Stadium': 17, 'Sardar Patel Stadium': 18, 'Barabati Stadium': 19, 'Vidarbha Cricket Association Stadium': 20,
            'Himachal Pradesh Cricket Association Stadium': 21, 'Nehru Stadium': 22, 'Holkar Cricket Stadium': 23, 'Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium': 24, 'Subrata Roy Sahara Stadium': 25,
            'Shaheed Veer Narayan Singh International Stadium': 26, 'JSCA International Stadium Complex': 27, 'Sheikh Zayed Stadium': 28, 'Sharjah Cricket Stadium': 29, 'Dubai International Cricket Stadium': 30,
            'Maharashtra Cricket Association Stadium': 31, 'Saurashtra Cricket Association Stadium': 32, 'Green Park': 33, 'Arun Jaitley Stadium': 34}

team_id = {'Royal Challengers Bangalore': 0, 'Kolkata Knight Riders': 1, 'Punjab Kings': 2, 'Chennai Super Kings': 3, 'Delhi Capitals': 4, 'Rajasthan Royals': 5, 'Mumbai Indians': 6, 'Deccan Chargers': 7, 'Kochi Tuskers Kerala': 8, 'Pune Warriors': 9,
           'Sunrisers Hyderabad': 10, 'Rising Pune Supergiant': 11, 'Gujarat Lions': 12}

venue_na = {'avg_wicket_20': 5.840249560599999, 'max_wicket_6': 3.942857142857143, 'avg_wicket_6': 1.5042576654857143, 'overall_sr_6': 119.462, 'avg_runs_6': 89.833091148}

def preprocess(DataFrame, data, colTransformer, drop_target=False):
    venue = pd.read_csv('https://raw.githubusercontent.com/kritikseth/Datasets/master/IIT-Madras-IPL/venue.csv')
    venue_innings_battingteam = pd.read_csv('https://raw.githubusercontent.com/kritikseth/Datasets/master/IIT-Madras-IPL/venue_innings_battingteam.csv')
    venue_innings_bowlingteam = pd.read_csv('https://raw.githubusercontent.com/kritikseth/Datasets/master/IIT-Madras-IPL/venue_innings_bowlingteam.csv')
    venue_oppteam_batsmen = pd.read_csv('https://raw.githubusercontent.com/kritikseth/Datasets/master/IIT-Madras-IPL/venue_oppteam_batsmen.csv')
    venue_oppteam_bowlers = pd.read_csv('https://raw.githubusercontent.com/kritikseth/Datasets/master/IIT-Madras-IPL/venue_oppteam_bowler.csv')

    df = DataFrame
    df.value = data

    df.add_cols(venue, prefix='venue.', on=['venue'])
    df.add_cols(venue_innings_battingteam, prefix='venue_innings_battingteam.', on=['venue', 'innings', 'batting_team'])
    df.add_cols(venue_innings_bowlingteam, prefix='venue_innings_bowlingteam.', on=['venue', 'innings', 'bowling_team'])

    df.split_add(venue_oppteam_batsmen, 'batsmen', 'venue_oppteam_batsmen.', on=['venue', 'bowling_team', 'batsmen'])
    df.split_add(venue_oppteam_bowlers, 'bowlers', 'venue_oppteam_bowlers.', on=['venue', 'batting_team', 'bowlers'])

    data = df.value

    corr_cols = ['venue_oppteam_bowlers.min_wickets_6', 'venue_oppteam_bowlers.max_wickets_6', 'venue_oppteam_batsmen.min_runs_6', 'venue_oppteam_batsmen.max_runs_6', 'venue.overall_sr_6',
                 'venue_innings_battingteam.max_runs_6', 'venue_innings_battingteam.min_runs_6', 'venue_innings_battingteam.avg_runs_20', 'venue_innings_battingteam.strikerate_20', 'venue_oppteam_bowlers.innings',
                 'venue_innings_bowlingteam.max_wickets_6', 'venue_innings_bowlingteam.avg_wickets_20', 'venue.avg_wicket_20', 'venue.max_wicket_6']
    data.drop(corr_cols, axis=1, inplace=True)
    
    if drop_target:
        X = data.drop(['total_runs'], axis=1)
    else:
        X = data

    na_cols = X[X.columns[X.isna().any()]].columns.tolist()

    for col in na_cols:

        if col.split('.')[0] == 'venue':
            X[col] = venue_na[col.split('.')[1]]
        
        elif col.split('.')[0] == 'venue_innings_battingteam':
            vibatna = pd.read_csv('https://raw.githubusercontent.com/kritikseth/Datasets/master/IIT-Madras-IPL/vibatna.csv')
            vibatna = vibatna[vibatna['venue']==X['venue'][0]]
            vibatna = vibatna[vibatna['innings']==X['innings'][0]]
            X[col] = vibatna[col.split('.')[1]].tolist()[0]
        
        elif col.split('.')[0] == 'venue_innings_bowlingteam':
            vibotna = pd.read_csv('https://raw.githubusercontent.com/kritikseth/Datasets/master/IIT-Madras-IPL/vibotna.csv')
            vibotna = vibotna[vibotna['venue']==X['venue'][0]]
            vibotna = vibotna[vibotna['innings']==X['innings'][0]]
            X[col] = vibotna[col.split('.')[1]].tolist()[0]
        
        elif col.split('.')[0] == 'venue_oppteam_batsmen':
            votbana = pd.read_csv('https://raw.githubusercontent.com/kritikseth/Datasets/master/IIT-Madras-IPL/votbana.csv')
            votbana = votbana[votbana['venue']==X['venue'][0]]
            votbana = votbana[votbana['bowling_team']==X['bowling_team'][0]]
            X[col] = votbana[col.split('.')[1]].tolist()[0]
        
        elif col.split('.')[0] == 'venue_oppteam_bowlers':
            votbona = pd.read_csv('https://raw.githubusercontent.com/kritikseth/Datasets/master/IIT-Madras-IPL/votbona.csv')
            votbona = votbona[votbona['venue']==X['venue'][0]]
            votbona = votbona[votbona['batting_team']==X['batting_team'][0]]
            X[col] = votbona[col.split('.')[1]].tolist()[0]

    X['venue'] = X['venue'].replace(venue_id)
    X['batting_team'] = X['batting_team'].replace(team_id)
    X['bowling_team'] = X['bowling_team'].replace(team_id)

    X.iloc[:, [5, 6, 7, 8, 12, 13, 15, 17]] = colTransformer.transform(X)

    return X

__all__ = ['DataFrame', 'venue_id', 'team_id', 'preprocess']
