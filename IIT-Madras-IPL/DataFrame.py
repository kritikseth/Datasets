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
        self.__df[playerCol] = train[playerCol].apply(lambda x: x.split(', '))
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
        
__all__ = ['DataFrame']
