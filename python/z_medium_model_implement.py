# -*- coding: utf-8 -*-
"""
"""

import os
import pickle
import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor

working_directory = 'C:/Users/'
data_dir = 'Data/'
etl_dir = 'Data/ETL'
os.chdir(working_directory)



curr_wk = 17 #NFL WEEK You Will Be Creating Predictions For
""" YOU NEED TO UPDATE THE SCHEDULE FILE TO INCLUDE CURRENT WEEK GAMES """

player_stats = pd.read_csv(data_dir + 'player_stats.csv', index_col = 0)
sched = player_stats[['Season','Week','Team','Opponent']].drop_duplicates().reset_index(drop=True).rename(columns={'Opponent':'Defense'})
sched = sched.sort_values(['Season','Week'], ascending = [False, False]).reset_index(drop=True)
sched.to_csv(data_dir + 'schedule_so_far.csv')

""" MAKE SURE TO ADD IN WEEK X GAMES TO FILE """
sched_update = pd.read_csv(data_dir + 'schedule_so_far_updated.csv', index_col = 0)
sched_update['Season'] = sched_update['Season'].fillna(0)
sched_update[['Season','Week']] = sched_update[['Season','Week']].astype(int)


players_df = player_stats[['Team','PlayerID']].drop_duplicates()
curr_wk_df = sched_update[(sched_update['Season']==2020) & (sched_update['Week']==curr_wk)].copy().reset_index(drop=True)
curr_wk_players = pd.merge(curr_wk_df, players_df, how = 'left', on = ['Team'])

qb_features = ['Rya3', 'def_Pc_pg', 'Py_pg', 'DkPtsDiff', 'Ra_pg', 'def_Qb_DKPtsRank', 
               'def_Pa', 'Qb_TDpRank', 'def_Qb_DKPts3', 'def_Rya3']

qb_vs = pd.read_csv(etl_dir + 'qb_stats.csv', index_col = 0)
qb_vs = qb_vs.rename(columns={'Opponent':'Defense'})
qb_vs['DkRankDiff'] = qb_vs['def_Qb_DKPtsRank'] - qb_vs['Qb_DKPtsRank'] 
qb_vs['DkPtsDiff'] = qb_vs['def_Qb_DKPts'] - qb_vs['Qb_DKPts'] 
need_dummy_cols = list(set(qb_vs.columns.tolist()) - set(curr_wk_players.columns.tolist()))

for col in need_dummy_cols:
    curr_wk_players[col] = 0.0
    
qb_vs = qb_vs[qb_vs['Season']==2020].reset_index(drop=True)
curr_qb_vs = pd.concat([qb_vs, curr_wk_players])

curr_qb_vs['Games3'] = curr_qb_vs.groupby(['PlayerID'])['Played'].transform(lambda x: x.shift().rolling(3).sum())
curr_qb_vs['Pc3'] = curr_qb_vs.groupby(['PlayerID'])['PassingCompletions'].transform(lambda x: x.shift().rolling(3).sum())
curr_qb_vs['Pc_pg3'] = curr_qb_vs.groupby(['PlayerID'])['PassingCompletions'].transform(lambda x: x.shift().rolling(3).mean())
curr_qb_vs['Pa3'] = curr_qb_vs.groupby(['PlayerID'])['PassingAttempts'].transform(lambda x: x.shift().rolling(3).sum())
curr_qb_vs['Pa_pg3'] = curr_qb_vs.groupby(['PlayerID'])['PassingAttempts'].transform(lambda x: x.shift().rolling(3).mean())
curr_qb_vs['Py3'] = curr_qb_vs.groupby(['PlayerID'])['PassingYards'].transform(lambda x: x.shift().rolling(3).sum())
curr_qb_vs['Py_pg3'] = curr_qb_vs.groupby(['PlayerID'])['PassingYards'].transform(lambda x: x.shift().rolling(3).mean())
curr_qb_vs['Pya3'] = curr_qb_vs['Py3'] / curr_qb_vs['Pa3']
curr_qb_vs['Pc%3'] = curr_qb_vs['Pc3'] / curr_qb_vs['Pa3']
curr_qb_vs['TDpass3'] = curr_qb_vs.groupby(['PlayerID'])['PassingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
curr_qb_vs['Ptd_pg3'] = curr_qb_vs.groupby(['PlayerID'])['PassingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
curr_qb_vs['Int3'] = curr_qb_vs.groupby(['PlayerID'])['PassingInterceptions'].transform(lambda x: x.shift().rolling(3).sum())
curr_qb_vs['Int_pg3'] = curr_qb_vs.groupby(['PlayerID'])['PassingInterceptions'].transform(lambda x: x.shift().rolling(3).mean())

curr_qb_vs['Ra3'] = curr_qb_vs.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).sum())
curr_qb_vs['Ra_pg3'] = curr_qb_vs.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).mean())
curr_qb_vs['Ry3'] = curr_qb_vs.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().rolling(3).sum())
curr_qb_vs['Ry_pg3'] = curr_qb_vs.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().rolling(3).mean())
curr_qb_vs['Rya3'] = curr_qb_vs['Ry3'] / curr_qb_vs['Ra3']
curr_qb_vs['TDrush3'] = curr_qb_vs.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
curr_qb_vs['Rtd_pg3'] = curr_qb_vs.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
curr_qb_vs['FumLost3'] = curr_qb_vs.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).sum())
curr_qb_vs['Fum_pg3'] = curr_qb_vs.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).mean())

curr_qb_vs = curr_qb_vs.fillna(0)

curr_qb_vs['Qb_PcRank3'] = curr_qb_vs.groupby(['Week'])['Pc_pg3'].rank(method='min')
curr_qb_vs['Qb_PaRank3'] = curr_qb_vs.groupby(['Week'])['Pa_pg3'].rank(method='min')
curr_qb_vs['Qb_PcpRank3'] = curr_qb_vs.groupby(['Week'])['Pc%3'].rank(method='min')
curr_qb_vs['Qb_PyRank3'] = curr_qb_vs.groupby(['Week'])['Py_pg3'].rank(method='min')
curr_qb_vs['Qb_PyaRank3'] = curr_qb_vs.groupby(['Week'])['Pya3'].rank(method='min')
curr_qb_vs['Qb_TDpRank3'] = curr_qb_vs.groupby(['Week'])['Ptd_pg3'].rank(method='min')
curr_qb_vs['Qb_IntRank3'] = curr_qb_vs.groupby(['Week'])['Int_pg3'].rank(method='min', ascending = False)
curr_qb_vs['Qb_RaRank3'] = curr_qb_vs.groupby(['Week'])['Ra_pg3'].rank(method='min')
curr_qb_vs['Qb_RyRank3'] = curr_qb_vs.groupby(['Week'])['Ry_pg3'].rank(method='min')
curr_qb_vs['Qb_RyaRank3'] = curr_qb_vs.groupby(['Week'])['Rya3'].rank(method='min')
curr_qb_vs['Qb_TDrushRank3'] = curr_qb_vs.groupby(['Week'])['Rtd_pg3'].rank(method='min')

curr_qb_vs['Qb_DKPts3'] = (curr_qb_vs['Ry3'] * .1 + curr_qb_vs['TDrush3'] * 6 +\
                       curr_qb_vs['Py3'] * .04 + curr_qb_vs['TDpass3'] * 4 +\
                       curr_qb_vs['Int3'] * -1 + curr_qb_vs['FumLost3'] * -1) / curr_qb_vs['Games3']

curr_qb_vs['Qb_DKPtsRank3'] = curr_qb_vs.groupby(['Week'])['Qb_DKPts3'].rank(method='min', ascending = False)


curr_qb_vs['Games'] = curr_qb_vs.groupby(['PlayerID'])['Played'].transform(lambda x: x.shift().sum())
curr_qb_vs['Pc'] = curr_qb_vs.groupby(['PlayerID'])['PassingCompletions'].transform(lambda x: x.shift().sum())
curr_qb_vs['Pc_pg'] = curr_qb_vs.groupby(['PlayerID'])['PassingCompletions'].transform(lambda x: x.shift().mean())
curr_qb_vs['Pa'] = curr_qb_vs.groupby(['PlayerID'])['PassingAttempts'].transform(lambda x: x.shift().sum())
curr_qb_vs['Pa_pg'] = curr_qb_vs.groupby(['PlayerID'])['PassingAttempts'].transform(lambda x: x.shift().mean())
curr_qb_vs['Py'] = curr_qb_vs.groupby(['PlayerID'])['PassingYards'].transform(lambda x: x.shift().sum())
curr_qb_vs['Py_pg'] = curr_qb_vs.groupby(['PlayerID'])['PassingYards'].transform(lambda x: x.shift().mean())
curr_qb_vs['Pya'] = curr_qb_vs['Py'] / curr_qb_vs['Pa']
curr_qb_vs['Pc%'] = curr_qb_vs['Pc'] / curr_qb_vs['Pa']
curr_qb_vs['TDpass'] = curr_qb_vs.groupby(['PlayerID'])['PassingTouchdowns'].transform(lambda x: x.shift().sum())
curr_qb_vs['Ptd_pg'] = curr_qb_vs.groupby(['PlayerID'])['PassingTouchdowns'].transform(lambda x: x.shift().mean())
curr_qb_vs['Int'] = curr_qb_vs.groupby(['PlayerID'])['PassingInterceptions'].transform(lambda x: x.shift().sum())
curr_qb_vs['Int_pg'] = curr_qb_vs.groupby(['PlayerID'])['PassingInterceptions'].transform(lambda x: x.shift().mean())

curr_qb_vs['Ra'] = curr_qb_vs.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().sum())
curr_qb_vs['Ra_pg'] = curr_qb_vs.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().mean())
curr_qb_vs['Ry'] = curr_qb_vs.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().sum())
curr_qb_vs['Ry_pg'] = curr_qb_vs.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().mean())
curr_qb_vs['Rya'] = curr_qb_vs['Ry'] / curr_qb_vs['Ra']
curr_qb_vs['TDrush'] = curr_qb_vs.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().sum())
curr_qb_vs['Rtd_pg'] = curr_qb_vs.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().mean())
curr_qb_vs['FumLost'] = curr_qb_vs.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().sum())
curr_qb_vs['Fum_pg'] = curr_qb_vs.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().mean())

curr_qb_vs = curr_qb_vs.fillna(0)

curr_qb_vs['Qb_PcRank'] = curr_qb_vs.groupby(['Week'])['Pc_pg'].rank(method='min')
curr_qb_vs['Qb_PaRank'] = curr_qb_vs.groupby(['Week'])['Pa_pg'].rank(method='min')
curr_qb_vs['Qb_PcpRank'] = curr_qb_vs.groupby(['Week'])['Pc%'].rank(method='min')
curr_qb_vs['Qb_PyRank'] = curr_qb_vs.groupby(['Week'])['Py_pg'].rank(method='min')
curr_qb_vs['Qb_PyaRank'] = curr_qb_vs.groupby(['Week'])['Pya'].rank(method='min')
curr_qb_vs['Qb_TDpRank'] = curr_qb_vs.groupby(['Week'])['Ptd_pg'].rank(method='min')
curr_qb_vs['Qb_IntRank'] = curr_qb_vs.groupby(['Week'])['Int_pg'].rank(method='min', ascending = False)
curr_qb_vs['Qb_RaRank'] = curr_qb_vs.groupby(['Week'])['Ra_pg'].rank(method='min')
curr_qb_vs['Qb_RyRank'] = curr_qb_vs.groupby(['Week'])['Ry_pg'].rank(method='min')
curr_qb_vs['Qb_RyaRank'] = curr_qb_vs.groupby(['Week'])['Rya'].rank(method='min')
curr_qb_vs['Qb_TDrushRank'] = curr_qb_vs.groupby(['Week'])['Rtd_pg'].rank(method='min')
curr_qb_vs['Qb_DKPts'] = (curr_qb_vs['Ry'] * .1 + curr_qb_vs['TDrush'] * 6 +\
                       curr_qb_vs['Py'] * .04 + curr_qb_vs['TDpass'] * 4 +\
                       curr_qb_vs['Int'] * -1 + curr_qb_vs['FumLost'] * -1)

curr_qb_vs['Qb_DKPtsRank'] = curr_qb_vs['Qb_DKPts'].rank(method='min', ascending = False)  
curr_qb_vs['DkRankDiff'] = curr_qb_vs['def_Qb_DKPtsRank'] - curr_qb_vs['Qb_DKPtsRank'] 
curr_qb_vs['DkPtsDiff'] = curr_qb_vs['def_Qb_DKPts'] - curr_qb_vs['Qb_DKPts'] 


pred_df = curr_qb_vs[curr_qb_vs['Week']==curr_wk].copy().reset_index(drop=True)
X_test = pred_df[qb_features].copy()


pkl_filename = etl_dir + "qb_model_col10.pkl"
with open(pkl_filename, 'rb') as file:
    model = pickle.load(file)

predictions = model.predict(X_test)
pred_df['Prediction'] = predictions
pred_df['PredictRank'] = pred_df.groupby(['Season','Week'])['Prediction'].rank(method='min')

out_cols = ['Season','Week','PlayerID','Team','Defense','PredictRank']
pred_df = pred_df[out_cols].to_csv(data_dir + 'Qb_Predictions_Wk'+str(curr_wk)+'.csv')







