# -*- coding: utf-8 -*-
"""
"""

import os
import pickle
import numpy as np
import pandas as pd
import xgboost as xgb
from lightgbm import LGBMRegressor
from sklearn.feature_selection import RFE

working_directory = 'C:/Users/'
data_dir = 'Data/'
etl_dir = 'Data/ETL'


def lgbm_mod(): return LGBMRegressor(random_state = 1)
def xgb_mod(): return xgb.XGBRegressor(random_state = 1)



player_stats = pd.read_csv(data_dir + 'player_stats.csv', index_col = 0)
qb_vs = pd.read_csv(etl_dir + 'qb_stats.csv', index_col = 0)

qb_vs = qb_vs.rename(columns={'Opponent':'Defense'})
qb_vs['DkRankDiff'] = qb_vs['def_Qb_DKPtsRank'] - qb_vs['Qb_DKPtsRank'] 
qb_vs['DkPtsDiff'] = qb_vs['def_Qb_DKPts'] - qb_vs['Qb_DKPts'] 

print(qb_vs.columns.tolist())

qb_act_stats = player_stats[player_stats['Position']=='QB'][['Season','Week','PlayerID','PassingCompletions', 'PassingAttempts', 'PassingCompletionPercentage', 'PassingYards', 'PassingYardsPerAttempt', 'PassingTouchdowns', 'PassingInterceptions', 'RushingAttempts', 'RushingYards', 'RushingTouchdowns','FumblesLost']].copy().reset_index(drop=True)
qb_act_stats = qb_act_stats.fillna(0)
qb_act_stats['Act_QB_DKPts'] = (qb_act_stats['RushingYards'] * .1 + qb_act_stats['RushingTouchdowns'] * 6 +\
                            qb_act_stats['PassingYards'] * .04 + qb_act_stats['PassingTouchdowns'] * 4 +\
                            qb_act_stats['PassingInterceptions'] * -1 + qb_act_stats['FumblesLost'] * -1)
qb_act_stats['Act_QB_DKPtsRank'] = qb_act_stats.groupby(['Season','Week'])['Act_QB_DKPts'].rank(method='min', ascending = False)

keep_cols = ['Season','Week','PlayerID','Act_QB_DKPtsRank','Act_QB_DKPts']
qb_vs_act = pd.merge(qb_vs, qb_act_stats[keep_cols], how = 'left', on = ['Season','Week','PlayerID'])
qb_vs_act = qb_vs_act[qb_vs_act['Act_QB_DKPts']>0].reset_index(drop=True)
qb_vs_act = qb_vs_act.loc[:,~qb_vs_act.columns.duplicated()]
qb_vs_act = qb_vs_act.replace([np.inf, -np.inf], np.nan)
qb_vs_act.to_csv(etl_dir + 'qb_v_def_stats.csv')

traindf = qb_vs_act[(qb_vs_act['Season']<=2019)].fillna(0).copy().drop_duplicates().reset_index(drop=True)
testdf  = qb_vs_act[qb_vs_act['Season']>=2020].fillna(0).copy().drop_duplicates().reset_index(drop=True)

dcols = ['Act_QB_DKPtsRank','Act_QB_DKPts','Defense','IsFavorite','TeamHasPossession', 'HomeOrAway', 'TeamIsHome', 'Result', 'HomeScore', 'AwayScore', 'Quarter', 'QuarterDisplay', 'IsGameOver', 'GameDate', 'TimeRemaining', 'ScoreSummary', 'PassingCompletions', 'PassingAttempts', 'PassingCompletionPercentage', 'PassingYards', 'PassingYardsPerAttempt', 'PassingTouchdowns', 'PassingInterceptions', 'PassingRating', 'RushingAttempts', 'RushingYards', 'RushingYardsPerAttempt', 'RushingTouchdowns', 'Receptions', 'ReceivingTargets', 'ReceivingYards', 'ReceptionPercentage', 'ReceivingTouchdowns', 'ReceivingLong', 'ReceivingYardsPerTarget', 'ReceivingYardsPerReception', 'Fumbles', 'FumblesLost', 'FieldGoalsMade', 'FieldGoalsAttempted', 'FieldGoalPercentage', 'FieldGoalsLongestMade', 'ExtraPointsMade', 'ExtraPointsAttempted', 'TacklesForLoss', 'Sacks', 'QuarterbackHits', 'Interceptions', 'FumblesRecovered', 'Safeties', 'DefensiveTouchdowns', 'SpecialTeamsTouchdowns', 'SoloTackles', 'AssistedTackles', 'SackYards', 'PassesDefended', 'FumblesForced', 'FantasyPoints', 'FantasyPointsPPR', 'FantasyPointsFanDuel', 'FantasyPointsYahoo', 'FantasyPointsFantasyDraft', 'FantasyPointsDraftKings', 'FantasyPointsHalfPointPpr', 'FantasyPointsSixPointPassTd', 'FantasyPointsPerGame', 'FantasyPointsPerGamePPR', 'FantasyPointsPerGameFanDuel', 'FantasyPointsPerGameYahoo', 'FantasyPointsPerGameDraftKings', 'FantasyPointsPerGameHalfPointPPR', 'FantasyPointsPerGameSixPointPassTd', 'FantasyPointsPerGameFantasyDraft', 'PlayerUrlString', 'GameStatus', 'GameStatusClass', 'PointsAllowedByDefenseSpecialTeams', 'TotalTackles', 'StatSummary', 'Name', 'ShortName', 'FirstName', 'LastName', 'FantasyPosition', 'Position', 'TeamUrlString', 'Team', 'IsScrambled', 'Rank', 'StaticRank', 'PositionRank','Played', 'Started','Season','Week']

X_train, Y_train = traindf.drop(dcols, axis = 1), traindf['Act_QB_DKPtsRank']
X_test, Y_test = testdf.drop(dcols, axis = 1), testdf['Act_QB_DKPtsRank']
pred_df = testdf.copy()


#print(X_train.columns.tolist())
print('Num Possible Features:',len(X_train.columns.tolist()))
""" LGBM Regressor """

model = lgbm_mod()
model.fit(X_train, Y_train)
dset = pd.DataFrame({'attr':X_train.columns.tolist(),'importance':model.feature_importances_}).sort_values(by='importance', ascending=False).reset_index(drop=True)
attr50 = dset['attr'][0:50].tolist()

model.fit(X_train[attr50], Y_train)
dset = pd.DataFrame({'attr':X_train[attr50].columns.tolist(),'importance':model.feature_importances_}).sort_values(by='importance', ascending=False).reset_index(drop=True)
attr30 = dset['attr'][0:30].tolist()

model.fit(X_train[attr30], Y_train)
dset = pd.DataFrame({'attr':X_train[attr30].columns.tolist(),'importance':model.feature_importances_}).sort_values(by='importance', ascending=False).reset_index(drop=True)
attr20 = dset['attr'][0:20].tolist()

rfe_model = RFE(model, n_features_to_select = 15)
rfe_model.fit(X_train[attr20], Y_train)
dset = pd.DataFrame({'attr':X_train[attr20].columns.tolist(),'importance':rfe_model.ranking_}).sort_values(by='importance', ascending=False).reset_index(drop=True)
cols15 = dset[dset['importance']==1]['attr'].tolist()

rfe_model = RFE(model, n_features_to_select = 10)
rfe_model.fit(X_train[cols15], Y_train)
dset = pd.DataFrame({'attr':X_train[cols15].columns.tolist(),'importance':rfe_model.ranking_}).sort_values(by='importance', ascending=False).reset_index(drop=True)
cols10 = dset[dset['importance']==1]['attr'].tolist()

print(attr20)
print(cols15)
print(cols10)


model.fit(X_train[attr20], Y_train)
preds20 = model.predict(X_test[attr20])
model.fit(X_train[cols15], Y_train)
preds15 = model.predict(X_test[cols15])
model.fit(X_train[cols10], Y_train)
preds10 = model.predict(X_test[cols10])

pdf = pred_df[['Season','Week','Team','Defense','PlayerID','Name','Act_QB_DKPtsRank','Act_QB_DKPts']].copy()
pdf['Prediction20'] = preds20
pdf['Predicted20Rank'] = pdf.groupby(['Season','Week'])['Prediction20'].rank(method='min')
pdf['Prediction15'] = preds15
pdf['Predicted15Rank'] = pdf.groupby(['Season','Week'])['Prediction15'].rank(method='min')
pdf['Prediction10'] = preds10
pdf['Predicted10Rank'] = pdf.groupby(['Season','Week'])['Prediction10'].rank(method='min')
pdf.to_csv(etl_dir + 'qb_predictions_medium_20_15_10.csv')

temp_df20 = pdf[pdf['Predicted20Rank']<=5]
print('% Of Top 5 Predicted QBS That Actually Hit Top5:',100*(temp_df20[temp_df20['Act_QB_DKPtsRank']<=5].shape[0] / temp_df20.shape[0]))






















