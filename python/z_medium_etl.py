"""
"""


import os
import pandas as pd

# Input Your Working Directory Is
# Where Your Data Directory Is
# Where You Want To Output ETL Data
working_directory = 'C:/Users/'
data_dir = 'Data/'
etl_dir = 'Data/ETL'

os.chdir(working_directory)

""" How Does Performance Get Affected By The Defense They Are Playing """
player_stats = pd.read_csv(data_dir + 'player_stats.csv', index_col = 0)

""" OFFENSE QUARTERBACKS ETL """
qb_yr_dfs = []
for yr in range(2002, 2021): 
    yr_qb = player_stats[(player_stats['Season']==yr) & (player_stats['Position']=='QB')].copy().reset_index(drop=True)
    yr_qb.sort_values(['PlayerID', 'Week'], ascending = [True, True], inplace = True)
    
    yr_qb['Games3'] = yr_qb.groupby(['PlayerID'])['Played'].transform(lambda x: x.shift().rolling(3).sum())
    yr_qb['Pc3'] = yr_qb.groupby(['PlayerID'])['PassingCompletions'].transform(lambda x: x.shift().rolling(3).sum())
    yr_qb['Pc_pg3'] = yr_qb.groupby(['PlayerID'])['PassingCompletions'].transform(lambda x: x.shift().rolling(3).mean())
    yr_qb['Pa3'] = yr_qb.groupby(['PlayerID'])['PassingAttempts'].transform(lambda x: x.shift().rolling(3).sum())
    yr_qb['Pa_pg3'] = yr_qb.groupby(['PlayerID'])['PassingAttempts'].transform(lambda x: x.shift().rolling(3).mean())
    yr_qb['Py3'] = yr_qb.groupby(['PlayerID'])['PassingYards'].transform(lambda x: x.shift().rolling(3).sum())
    yr_qb['Py_pg3'] = yr_qb.groupby(['PlayerID'])['PassingYards'].transform(lambda x: x.shift().rolling(3).mean())
    yr_qb['Pya3'] = yr_qb['Py3'] / yr_qb['Pa3']
    yr_qb['Pc%3'] = yr_qb['Pc3'] / yr_qb['Pa3']
    yr_qb['TDpass3'] = yr_qb.groupby(['PlayerID'])['PassingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    yr_qb['Ptd_pg3'] = yr_qb.groupby(['PlayerID'])['PassingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    yr_qb['Int3'] = yr_qb.groupby(['PlayerID'])['PassingInterceptions'].transform(lambda x: x.shift().rolling(3).sum())
    yr_qb['Int_pg3'] = yr_qb.groupby(['PlayerID'])['PassingInterceptions'].transform(lambda x: x.shift().rolling(3).mean())
    
    yr_qb['Ra3'] = yr_qb.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).sum())
    yr_qb['Ra_pg3'] = yr_qb.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).mean())
    yr_qb['Ry3'] = yr_qb.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().rolling(3).sum())
    yr_qb['Ry_pg3'] = yr_qb.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().rolling(3).mean())
    yr_qb['Rya3'] = yr_qb['Ry3'] / yr_qb['Ra3']
    yr_qb['TDrush3'] = yr_qb.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    yr_qb['Rtd_pg3'] = yr_qb.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    yr_qb['FumLost3'] = yr_qb.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).sum())
    yr_qb['Fum_pg3'] = yr_qb.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).mean())

    yr_qb = yr_qb.fillna(0)
    
    yr_qb['Qb_PcRank3'] = yr_qb.groupby(['Week'])['Pc_pg3'].rank(method='min')
    yr_qb['Qb_PaRank3'] = yr_qb.groupby(['Week'])['Pa_pg3'].rank(method='min')
    yr_qb['Qb_PcpRank3'] = yr_qb.groupby(['Week'])['Pc%3'].rank(method='min')
    yr_qb['Qb_PyRank3'] = yr_qb.groupby(['Week'])['Py_pg3'].rank(method='min')
    yr_qb['Qb_PyaRank3'] = yr_qb.groupby(['Week'])['Pya3'].rank(method='min')
    yr_qb['Qb_TDpRank3'] = yr_qb.groupby(['Week'])['Ptd_pg3'].rank(method='min')
    yr_qb['Qb_IntRank3'] = yr_qb.groupby(['Week'])['Int_pg3'].rank(method='min', ascending = False)
    yr_qb['Qb_RaRank3'] = yr_qb.groupby(['Week'])['Ra_pg3'].rank(method='min')
    yr_qb['Qb_RyRank3'] = yr_qb.groupby(['Week'])['Ry_pg3'].rank(method='min')
    yr_qb['Qb_RyaRank3'] = yr_qb.groupby(['Week'])['Rya3'].rank(method='min')
    yr_qb['Qb_TDrushRank3'] = yr_qb.groupby(['Week'])['Rtd_pg3'].rank(method='min')
    
    yr_qb['Qb_DKPts3'] = (yr_qb['Ry3'] * .1 + yr_qb['TDrush3'] * 6 +\
                           yr_qb['Py3'] * .04 + yr_qb['TDpass3'] * 4 +\
                           yr_qb['Int3'] * -1 + yr_qb['FumLost3'] * -1) / yr_qb['Games3']

    yr_qb['Qb_DKPtsRank3'] = yr_qb.groupby(['Week'])['Qb_DKPts3'].rank(method='min', ascending = False)
    
    
    yr_qb['Games'] = yr_qb.groupby(['PlayerID'])['Played'].transform(lambda x: x.shift().sum())
    yr_qb['Pc'] = yr_qb.groupby(['PlayerID'])['PassingCompletions'].transform(lambda x: x.shift().sum())
    yr_qb['Pc_pg'] = yr_qb.groupby(['PlayerID'])['PassingCompletions'].transform(lambda x: x.shift().mean())
    yr_qb['Pa'] = yr_qb.groupby(['PlayerID'])['PassingAttempts'].transform(lambda x: x.shift().sum())
    yr_qb['Pa_pg'] = yr_qb.groupby(['PlayerID'])['PassingAttempts'].transform(lambda x: x.shift().mean())
    yr_qb['Py'] = yr_qb.groupby(['PlayerID'])['PassingYards'].transform(lambda x: x.shift().sum())
    yr_qb['Py_pg'] = yr_qb.groupby(['PlayerID'])['PassingYards'].transform(lambda x: x.shift().mean())
    yr_qb['Pya'] = yr_qb['Py'] / yr_qb['Pa']
    yr_qb['Pc%'] = yr_qb['Pc'] / yr_qb['Pa']
    yr_qb['TDpass'] = yr_qb.groupby(['PlayerID'])['PassingTouchdowns'].transform(lambda x: x.shift().sum())
    yr_qb['Ptd_pg'] = yr_qb.groupby(['PlayerID'])['PassingTouchdowns'].transform(lambda x: x.shift().mean())
    yr_qb['Int'] = yr_qb.groupby(['PlayerID'])['PassingInterceptions'].transform(lambda x: x.shift().sum())
    yr_qb['Int_pg'] = yr_qb.groupby(['PlayerID'])['PassingInterceptions'].transform(lambda x: x.shift().mean())
    
    yr_qb['Ra'] = yr_qb.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().sum())
    yr_qb['Ra_pg'] = yr_qb.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().mean())
    yr_qb['Ry'] = yr_qb.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().sum())
    yr_qb['Ry_pg'] = yr_qb.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().mean())
    yr_qb['Rya'] = yr_qb['Ry'] / yr_qb['Ra']
    yr_qb['TDrush'] = yr_qb.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().sum())
    yr_qb['Rtd_pg'] = yr_qb.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().mean())
    yr_qb['FumLost'] = yr_qb.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().sum())
    yr_qb['Fum_pg'] = yr_qb.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().mean())

    yr_qb = yr_qb.fillna(0)
    
    yr_qb['Qb_PcRank'] = yr_qb.groupby(['Week'])['Pc_pg'].rank(method='min')
    yr_qb['Qb_PaRank'] = yr_qb.groupby(['Week'])['Pa_pg'].rank(method='min')
    yr_qb['Qb_PcpRank'] = yr_qb.groupby(['Week'])['Pc%'].rank(method='min')
    yr_qb['Qb_PyRank'] = yr_qb.groupby(['Week'])['Py_pg'].rank(method='min')
    yr_qb['Qb_PyaRank'] = yr_qb.groupby(['Week'])['Pya'].rank(method='min')
    yr_qb['Qb_TDpRank'] = yr_qb.groupby(['Week'])['Ptd_pg'].rank(method='min')
    yr_qb['Qb_IntRank'] = yr_qb.groupby(['Week'])['Int_pg'].rank(method='min', ascending = False)
    yr_qb['Qb_RaRank'] = yr_qb.groupby(['Week'])['Ra_pg'].rank(method='min')
    yr_qb['Qb_RyRank'] = yr_qb.groupby(['Week'])['Ry_pg'].rank(method='min')
    yr_qb['Qb_RyaRank'] = yr_qb.groupby(['Week'])['Rya'].rank(method='min')
    yr_qb['Qb_TDrushRank'] = yr_qb.groupby(['Week'])['Rtd_pg'].rank(method='min')
    yr_qb['Qb_DKPts'] = (yr_qb['Ry'] * .1 + yr_qb['TDrush'] * 6 +\
                           yr_qb['Py'] * .04 + yr_qb['TDpass'] * 4 +\
                           yr_qb['Int'] * -1 + yr_qb['FumLost'] * -1)

    yr_qb['Qb_DKPtsRank'] = yr_qb['Qb_DKPts'].rank(method='min', ascending = False)  
    qb_yr_dfs.append(yr_qb)
    
qb_stats = pd.concat(qb_yr_dfs).drop_duplicates().reset_index(drop=True)
qb_stats_all = pd.merge(qb_stats, player_stats[['Season','PlayerID','Team','Name']].drop_duplicates(), how = 'left', on = ['Season','PlayerID'])
qb_stats_all.to_csv(etl_dir + 'qb_stats.csv')


""" DEFENSE QUARTERBACKS ETL """
def_qb_yr_dfs = []
for yr in range(2002, 2021): 
    def_yr_qb = player_stats[(player_stats['Season']==yr) & (player_stats['Position']=='QB')].copy().reset_index(drop=True)
    def_yr_qb.sort_values(['PlayerID', 'Week'], ascending = [True, True], inplace = True)
    
    def_yr_qb['def_Games3'] = def_yr_qb.groupby(['Opponent'])['Played'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_qb['def_Pc3'] = def_yr_qb.groupby(['Opponent'])['PassingCompletions'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_qb['def_Pc_pg3'] = def_yr_qb.groupby(['Opponent'])['PassingCompletions'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_qb['def_Pa3'] = def_yr_qb.groupby(['Opponent'])['PassingAttempts'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_qb['def_Pa_pg3'] = def_yr_qb.groupby(['Opponent'])['PassingAttempts'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_qb['def_Py3'] = def_yr_qb.groupby(['Opponent'])['PassingYards'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_qb['def_Py_pg3'] = def_yr_qb.groupby(['Opponent'])['PassingYards'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_qb['def_Pya3'] = def_yr_qb['def_Py3'] / def_yr_qb['def_Pa3']
    def_yr_qb['def_Pc%3'] = def_yr_qb['def_Pc3'] / def_yr_qb['def_Pa3']
    def_yr_qb['def_TDpass3'] = def_yr_qb.groupby(['Opponent'])['PassingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_qb['def_Ptd_pg3'] = def_yr_qb.groupby(['Opponent'])['PassingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_qb['def_Int3'] = def_yr_qb.groupby(['Opponent'])['PassingInterceptions'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_qb['def_Int_pg3'] = def_yr_qb.groupby(['Opponent'])['PassingInterceptions'].transform(lambda x: x.shift().rolling(3).mean())
    
    def_yr_qb['def_Ra3'] = def_yr_qb.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_qb['def_Ra_pg3'] = def_yr_qb.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_qb['def_Ry3'] = def_yr_qb.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_qb['def_Ry_pg3'] = def_yr_qb.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_qb['def_Rya3'] = def_yr_qb['def_Ry3'] / def_yr_qb['def_Ra3']
    def_yr_qb['def_TDrush3'] = def_yr_qb.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_qb['def_Rtd_pg3'] = def_yr_qb.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_qb['def_FumLost3'] = def_yr_qb.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_qb['def_Fum_pg3'] = def_yr_qb.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).mean())

    def_yr_qb = def_yr_qb.fillna(0)
    
    def_yr_qb['def_Qb_PcRank3'] = def_yr_qb.groupby(['Week'])['def_Pc_pg3'].rank(method='min')
    def_yr_qb['def_Qb_PaRank3'] = def_yr_qb.groupby(['Week'])['def_Pa_pg3'].rank(method='min')
    def_yr_qb['def_Qb_PcpRank3'] = def_yr_qb.groupby(['Week'])['def_Pc%3'].rank(method='min')
    def_yr_qb['def_Qb_PyRank3'] = def_yr_qb.groupby(['Week'])['def_Py_pg3'].rank(method='min')
    def_yr_qb['def_Qb_PyaRank3'] = def_yr_qb.groupby(['Week'])['def_Pya3'].rank(method='min')
    def_yr_qb['def_Qb_TDpRank3'] = def_yr_qb.groupby(['Week'])['def_Ptd_pg3'].rank(method='min')
    def_yr_qb['def_Qb_IntRank3'] = def_yr_qb.groupby(['Week'])['def_Int_pg3'].rank(method='min', ascending = False)
    def_yr_qb['def_Qb_RaRank3'] = def_yr_qb.groupby(['Week'])['def_Ra_pg3'].rank(method='min')
    def_yr_qb['def_Qb_RyRank3'] = def_yr_qb.groupby(['Week'])['def_Ry_pg3'].rank(method='min')
    def_yr_qb['def_Qb_RyaRank3'] = def_yr_qb.groupby(['Week'])['def_Rya3'].rank(method='min')
    def_yr_qb['def_Qb_TDrushRank3'] = def_yr_qb.groupby(['Week'])['def_Rtd_pg3'].rank(method='min')
    def_yr_qb['def_Qb_DKPts3'] = (def_yr_qb['def_Ry3'] * .1 + def_yr_qb['def_TDrush3'] * 6 +\
                           def_yr_qb['def_Py3'] * .04 + def_yr_qb['def_TDpass3'] * 4 +\
                           def_yr_qb['def_Int3'] * -1 + def_yr_qb['def_FumLost3'] * -1) / def_yr_qb['def_Games3']
    def_yr_qb['def_Qb_DKPtsRank3'] = def_yr_qb.groupby(['Week'])['def_Qb_DKPts3'].rank(method='min', ascending = False)
        
    def_yr_qb['def_Pc'] = def_yr_qb.groupby(['Opponent'])['PassingCompletions'].transform(lambda x: x.shift().sum())
    def_yr_qb['def_Pc_pg'] = def_yr_qb.groupby(['Opponent'])['PassingCompletions'].transform(lambda x: x.shift().mean())
    def_yr_qb['def_Pa'] = def_yr_qb.groupby(['Opponent'])['PassingAttempts'].transform(lambda x: x.shift().sum())
    def_yr_qb['def_Pa_pg'] = def_yr_qb.groupby(['Opponent'])['PassingAttempts'].transform(lambda x: x.shift().mean())
    def_yr_qb['def_Py'] = def_yr_qb.groupby(['Opponent'])['PassingYards'].transform(lambda x: x.shift().sum())
    def_yr_qb['def_Py_pg'] = def_yr_qb.groupby(['Opponent'])['PassingYards'].transform(lambda x: x.shift().mean())
    def_yr_qb['def_Pya'] = def_yr_qb['def_Py'] / def_yr_qb['def_Pa']
    def_yr_qb['def_Pc%'] = def_yr_qb['def_Pc'] / def_yr_qb['def_Pa']
    def_yr_qb['def_TDpass'] = def_yr_qb.groupby(['Opponent'])['PassingTouchdowns'].transform(lambda x: x.shift().sum())
    def_yr_qb['def_Ptd_pg'] = def_yr_qb.groupby(['Opponent'])['PassingTouchdowns'].transform(lambda x: x.shift().mean())
    def_yr_qb['def_Int'] = def_yr_qb.groupby(['Opponent'])['PassingInterceptions'].transform(lambda x: x.shift().sum())
    def_yr_qb['def_Int_pg'] = def_yr_qb.groupby(['Opponent'])['PassingInterceptions'].transform(lambda x: x.shift().mean())
    
    def_yr_qb['def_Ra'] = def_yr_qb.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().sum())
    def_yr_qb['def_Ra_pg'] = def_yr_qb.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().mean())
    def_yr_qb['def_Ry'] = def_yr_qb.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().sum())
    def_yr_qb['def_Ry_pg'] = def_yr_qb.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().mean())
    def_yr_qb['def_Rya'] = def_yr_qb['def_Ry'] / def_yr_qb['def_Ra']
    def_yr_qb['def_TDrush'] = def_yr_qb.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().sum())
    def_yr_qb['def_Rtd_pg'] = def_yr_qb.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().mean())
    def_yr_qb['def_FumLost'] = def_yr_qb.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().sum())
    def_yr_qb['def_Fum_pg'] = def_yr_qb.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().mean())

    def_yr_qb = def_yr_qb.fillna(0)
    
    def_yr_qb['def_Qb_PcRank'] = def_yr_qb.groupby(['Week'])['def_Pc_pg'].rank(method='min')
    def_yr_qb['def_Qb_PaRank'] = def_yr_qb.groupby(['Week'])['def_Pa_pg'].rank(method='min')
    def_yr_qb['def_Qb_PcpRank'] = def_yr_qb.groupby(['Week'])['def_Pc%'].rank(method='min')
    def_yr_qb['def_Qb_PyRank'] = def_yr_qb.groupby(['Week'])['def_Py_pg'].rank(method='min')
    def_yr_qb['def_Qb_PyaRank'] = def_yr_qb.groupby(['Week'])['def_Pya'].rank(method='min')
    def_yr_qb['def_Qb_TDpRank'] = def_yr_qb.groupby(['Week'])['def_Ptd_pg'].rank(method='min')
    def_yr_qb['def_Qb_IntRank'] = def_yr_qb.groupby(['Week'])['def_Int_pg'].rank(method='min', ascending = False)
    def_yr_qb['def_Qb_RaRank'] = def_yr_qb.groupby(['Week'])['def_Ra_pg'].rank(method='min')
    def_yr_qb['def_Qb_RyRank'] = def_yr_qb.groupby(['Week'])['def_Ry_pg'].rank(method='min')
    def_yr_qb['def_Qb_RyaRank'] = def_yr_qb.groupby(['Week'])['def_Rya'].rank(method='min')
    def_yr_qb['def_Qb_TDrushRank'] = def_yr_qb.groupby(['Week'])['def_Rtd_pg'].rank(method='min')
    
    def_yr_qb['def_Qb_DKPts'] = (def_yr_qb['def_Ry'] * .1 + def_yr_qb['def_TDrush'] * 6 +\
                           def_yr_qb['def_Py'] * .04 + def_yr_qb['def_TDpass'] * 4 +\
                           def_yr_qb['def_Int'] * -1 + def_yr_qb['def_FumLost'] * -1)

    def_yr_qb['def_Qb_DKPtsRank'] = def_yr_qb.groupby(['Week'])['def_Qb_DKPts'].rank(method='min', ascending = False)  
    def_qb_yr_dfs.append(def_yr_qb)
    
def_qb_stats = pd.concat(def_qb_yr_dfs).drop_duplicates().reset_index(drop=True)
def_qb_stats.to_csv(etl_dir + 'def_qb_stats.csv')



""" OFFENSE RUNNING BACKS """
rb_yr_dfs = []
for yr in range(2002, 2021): 
    yr_rb = player_stats[(player_stats['Season']==yr) & (player_stats['Position']=='RB')].copy().reset_index(drop=True)
    yr_rb.sort_values(['PlayerID', 'Week'], ascending = [True, True], inplace = True)
    
    yr_rb['Games3'] = yr_rb.groupby(['PlayerID'])['Played'].transform(lambda x: x.shift().rolling(3).sum())
    yr_rb['Ra3'] = yr_rb.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).sum())
    yr_rb['Ra_pg3'] = yr_rb.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).mean())
    yr_rb['Ry3'] = yr_rb.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().rolling(3).sum())
    yr_rb['Ry_pg3'] = yr_rb.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().rolling(3).mean())
    yr_rb['Rya3'] = yr_rb['Ry3'] / yr_rb['Ra3']
    yr_rb['TDrush3'] = yr_rb.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    yr_rb['Rtd_pg3'] = yr_rb.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    
    yr_rb['Rec3'] = yr_rb.groupby(['PlayerID'])['Receptions'].transform(lambda x: x.shift().rolling(3).sum())
    yr_rb['Rec_pg3'] = yr_rb.groupby(['PlayerID'])['Receptions'].transform(lambda x: x.shift().rolling(3).mean())
    yr_rb['Tgt3'] = yr_rb.groupby(['PlayerID'])['ReceivingTargets'].transform(lambda x: x.shift().rolling(3).sum())
    yr_rb['Tgt_pg3'] = yr_rb.groupby(['PlayerID'])['ReceivingTargets'].transform(lambda x: x.shift().rolling(3).mean())
    yr_rb['Rec%3'] =  yr_rb['Rec3'] / yr_rb['Tgt3']
    yr_rb['Recy3'] = yr_rb.groupby(['PlayerID'])['ReceivingYards'].transform(lambda x: x.shift().rolling(3).sum())
    yr_rb['Recy_pg3'] = yr_rb.groupby(['PlayerID'])['ReceivingYards'].transform(lambda x: x.shift().rolling(3).mean())
    yr_rb['TDrec3'] = yr_rb.groupby(['PlayerID'])['ReceivingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    yr_rb['Rectd_pg3'] = yr_rb.groupby(['PlayerID'])['ReceivingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    yr_rb['YdsPerTgt3'] = yr_rb['Recy3'] / yr_rb['Tgt3']
    yr_rb['YdsPerRec3'] = yr_rb['Recy3'] / yr_rb['Rec3']
    
    yr_rb['FumLost3'] = yr_rb.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).sum())
    yr_rb['Fum_pg3'] = yr_rb.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).mean())
    yr_rb = yr_rb.fillna(0)

    yr_rb['Rb_RaRank3'] = yr_rb.groupby(['Week'])['Ra_pg3'].rank(method='min')
    yr_rb['Rb_RyRank3'] = yr_rb.groupby(['Week'])['Ry_pg3'].rank(method='min')
    yr_rb['Rb_RyaRank3'] = yr_rb.groupby(['Week'])['Rya3'].rank(method='min')
    yr_rb['Rb_TDrushRank3'] = yr_rb.groupby(['Week'])['Rtd_pg3'].rank(method='min')
    yr_rb['Rb_RecRank3'] = yr_rb.groupby(['Week'])['Rec_pg3'].rank(method='min')
    yr_rb['Rb_TgtRank3'] = yr_rb.groupby(['Week'])['Tgt_pg3'].rank(method='min')
    yr_rb['Rb_RecyRank3'] = yr_rb.groupby(['Week'])['Recy_pg3'].rank(method='min')
    yr_rb['Rb_Rec%Rank3'] = yr_rb.groupby(['Week'])['Rec%3'].rank(method='min')
    yr_rb['Rb_TDrecRank3'] = yr_rb.groupby(['Week'])['Rectd_pg3'].rank(method='min')
    yr_rb['Rb_YdsPerTgtRank3'] = yr_rb.groupby(['Week'])['YdsPerTgt3'].rank(method='min')
    yr_rb['Rb_YdsPerRecRank3'] = yr_rb.groupby(['Week'])['YdsPerRec3'].rank(method='min')
    yr_rb['Rb_FumRank3'] = yr_rb.groupby(['Week'])['Fum_pg3'].rank(method='min')        
    yr_rb['Rb_DKPts3'] =  (yr_rb['Ry3'] * .1 + yr_rb['TDrush3'] * 6 +\
                           yr_rb['Rec3'] * 1 + yr_rb['TDrec3'] * 6 +\
                           yr_rb['Recy3'] * .1 + yr_rb['FumLost3'] * -1) / yr_rb['Games3']
                              
    yr_rb['Rb_DKPtsRank3'] = yr_rb.groupby(['Week'])['Rb_DKPts3'].rank(method='min', ascending = False)
    
    yr_rb['Games'] = yr_rb.groupby(['PlayerID'])['Played'].transform(lambda x: x.shift().sum())
    yr_rb['Ra'] = yr_rb.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().sum())
    yr_rb['Ra_pg'] = yr_rb.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().mean())
    yr_rb['Ry'] = yr_rb.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().sum())
    yr_rb['Ry_pg'] = yr_rb.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().mean())
    yr_rb['Rya'] = yr_rb['Ry'] / yr_rb['Ra']
    yr_rb['TDrush'] = yr_rb.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().sum())
    yr_rb['Rtd_pg'] = yr_rb.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().mean())
    
    yr_rb['Rec'] = yr_rb.groupby(['PlayerID'])['Receptions'].transform(lambda x: x.shift().sum())
    yr_rb['Rec_pg'] = yr_rb.groupby(['PlayerID'])['Receptions'].transform(lambda x: x.shift().mean())
    yr_rb['Tgt'] = yr_rb.groupby(['PlayerID'])['ReceivingTargets'].transform(lambda x: x.shift().sum())
    yr_rb['Tgt_pg'] = yr_rb.groupby(['PlayerID'])['ReceivingTargets'].transform(lambda x: x.shift().mean())
    yr_rb['Rec%'] =  yr_rb['Rec'] / yr_rb['Tgt']
    yr_rb['Recy'] = yr_rb.groupby(['PlayerID'])['ReceivingYards'].transform(lambda x: x.shift().sum())
    yr_rb['Recy_pg'] = yr_rb.groupby(['PlayerID'])['ReceivingYards'].transform(lambda x: x.shift().mean())
    yr_rb['TDrec'] = yr_rb.groupby(['PlayerID'])['ReceivingTouchdowns'].transform(lambda x: x.shift().sum())
    yr_rb['Rectd_pg'] = yr_rb.groupby(['PlayerID'])['ReceivingTouchdowns'].transform(lambda x: x.shift().mean())
    yr_rb['YdsPerTgt'] = yr_rb['Recy'] / yr_rb['Tgt']
    yr_rb['YdsPerRec'] = yr_rb['Recy'] / yr_rb['Rec']
    
    yr_rb['FumLost'] = yr_rb.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().sum())
    yr_rb['Fum_pg'] = yr_rb.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().mean())
    yr_rb = yr_rb.fillna(0)

    yr_rb['Rb_RaRank'] = yr_rb.groupby(['Week'])['Ra_pg'].rank(method='min')
    yr_rb['Rb_RyRank'] = yr_rb.groupby(['Week'])['Ry_pg'].rank(method='min')
    yr_rb['Rb_RyaRank'] = yr_rb.groupby(['Week'])['Rya'].rank(method='min')
    yr_rb['Rb_TDrushRank'] = yr_rb.groupby(['Week'])['Rtd_pg'].rank(method='min')
    yr_rb['Rb_RecRank'] = yr_rb.groupby(['Week'])['Rec_pg'].rank(method='min')
    yr_rb['Rb_TgtRank'] = yr_rb.groupby(['Week'])['Tgt_pg'].rank(method='min')
    yr_rb['Rb_RecyRank'] = yr_rb.groupby(['Week'])['Recy_pg'].rank(method='min')
    yr_rb['Rb_Rec%Rank'] = yr_rb.groupby(['Week'])['Rec%'].rank(method='min')
    yr_rb['Rb_TDrecRank'] = yr_rb.groupby(['Week'])['Rectd_pg'].rank(method='min')
    yr_rb['Rb_YdsPerTgtRank'] = yr_rb.groupby(['Week'])['YdsPerTgt'].rank(method='min')
    yr_rb['Rb_YdsPerRecRank'] = yr_rb.groupby(['Week'])['YdsPerRec'].rank(method='min')
    yr_rb['Rb_FumRank'] = yr_rb.groupby(['Week'])['Fum_pg'].rank(method='min') 
       
    yr_rb['Rb_DKPts'] = (yr_rb['Ry'] * .1 + yr_rb['TDrush'] * 6 +\
                         yr_rb['Rec'] * 1 + yr_rb['TDrec'] * 6 +\
                         yr_rb['Recy'] * .1 + yr_rb['FumLost'] * -1)        
    yr_rb['Rb_DKPtsRank'] = yr_rb.groupby(['Week'])['Rb_DKPts'].rank(method='min', ascending = False)
    rb_yr_dfs.append(yr_rb)
    
rb_stats = pd.concat(rb_yr_dfs).drop_duplicates().reset_index(drop=True)
rb_stats_all = pd.merge(rb_stats, player_stats[['Season','PlayerID','Team','Name']].drop_duplicates(), how = 'left', on = ['Season','PlayerID'])
rb_stats_all.to_csv(etl_dir + 'rb_stats.csv')


""" DEFENSE RUNNING BACKS """
def_rb_yr_dfs = []
for yr in range(2002, 2021): 
    def_yr_rb = player_stats[(player_stats['Season']==yr) & (player_stats['Position']=='RB')].copy().reset_index(drop=True)
    def_yr_rb.sort_values(['PlayerID', 'Week'], ascending = [True, True], inplace = True)
    
    def_yr_rb['def_Games3'] = def_yr_rb.groupby(['Opponent'])['Played'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_rb['def_Ra3'] = def_yr_rb.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_rb['def_Ra_pg3'] = def_yr_rb.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_rb['def_Ry3'] = def_yr_rb.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_rb['def_Ry_pg3'] = def_yr_rb.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_rb['def_Rya3'] = def_yr_rb['def_Ry3'] / def_yr_rb['def_Ra3']
    def_yr_rb['def_TDrush3'] = def_yr_rb.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_rb['def_Rtd_pg3'] = def_yr_rb.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    
    def_yr_rb['def_Rec3'] = def_yr_rb.groupby(['Opponent'])['Receptions'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_rb['def_Rec_pg3'] = def_yr_rb.groupby(['Opponent'])['Receptions'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_rb['def_Tgt3'] = def_yr_rb.groupby(['Opponent'])['ReceivingTargets'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_rb['def_Tgt_pg3'] = def_yr_rb.groupby(['Opponent'])['ReceivingTargets'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_rb['def_Rec%3'] =  def_yr_rb['def_Rec3'] / def_yr_rb['def_Tgt3']
    def_yr_rb['def_Recy3'] = def_yr_rb.groupby(['Opponent'])['ReceivingYards'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_rb['def_Recy_pg3'] = def_yr_rb.groupby(['Opponent'])['ReceivingYards'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_rb['def_TDrec3'] = def_yr_rb.groupby(['Opponent'])['ReceivingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_rb['def_Rectd_pg3'] = def_yr_rb.groupby(['Opponent'])['ReceivingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_rb['def_YdsPerTgt3'] = def_yr_rb['def_Recy3'] / def_yr_rb['def_Tgt3']
    def_yr_rb['def_YdsPerRec3'] = def_yr_rb['def_Recy3'] / def_yr_rb['def_Rec3']
    
    def_yr_rb['def_FumLost3'] = def_yr_rb.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_rb['def_Fum_pg3'] = def_yr_rb.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_rb = def_yr_rb.fillna(0)

    def_yr_rb['def_Rb_RaRank3'] = def_yr_rb.groupby(['Week'])['def_Ra_pg3'].rank(method='min')
    def_yr_rb['def_Rb_RyRank3'] = def_yr_rb.groupby(['Week'])['def_Ry_pg3'].rank(method='min')
    def_yr_rb['def_Rb_RyaRank3'] = def_yr_rb.groupby(['Week'])['def_Rya3'].rank(method='min')
    def_yr_rb['def_Rb_TDrushRank3'] = def_yr_rb.groupby(['Week'])['def_Rtd_pg3'].rank(method='min')
    def_yr_rb['def_Rb_RecRank3'] = def_yr_rb.groupby(['Week'])['def_Rec_pg3'].rank(method='min')
    def_yr_rb['def_Rb_TgtRank3'] = def_yr_rb.groupby(['Week'])['def_Tgt_pg3'].rank(method='min')
    def_yr_rb['def_Rb_RecyRank3'] = def_yr_rb.groupby(['Week'])['def_Recy_pg3'].rank(method='min')
    def_yr_rb['def_Rb_Rec%Rank3'] = def_yr_rb.groupby(['Week'])['def_Rec%3'].rank(method='min')
    def_yr_rb['def_Rb_TDrecRank3'] = def_yr_rb.groupby(['Week'])['def_Rectd_pg3'].rank(method='min')
    def_yr_rb['def_Rb_YdsPerTgtRank3'] = def_yr_rb.groupby(['Week'])['def_YdsPerTgt3'].rank(method='min')
    def_yr_rb['def_Rb_YdsPerRecRank3'] = def_yr_rb.groupby(['Week'])['def_YdsPerRec3'].rank(method='min')
    def_yr_rb['def_Rb_FumRank3'] = def_yr_rb.groupby(['Week'])['def_Fum_pg3'].rank(method='min')     
    
    def_yr_rb['def_Rb_DKPts3'] =  (def_yr_rb['def_Ry3'] * .1 + def_yr_rb['def_TDrush3'] * 6 +\
                           def_yr_rb['def_Rec3'] * 1 + def_yr_rb['def_TDrec3'] * 6 +\
                           def_yr_rb['def_Recy3'] * .1 + def_yr_rb['def_FumLost3'] * -1) / def_yr_rb['def_Games3'] 
                              
    def_yr_rb['def_Rb_DKPtsRank3'] = def_yr_rb.groupby(['Week'])['def_Rb_DKPts3'].rank(method='min', ascending = False)
    
    def_yr_rb['def_Games'] = def_yr_rb.groupby(['Opponent'])['Played'].transform(lambda x: x.shift().sum())
    def_yr_rb['def_Ra'] = def_yr_rb.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().sum())
    def_yr_rb['def_Ra_pg'] = def_yr_rb.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().mean())
    def_yr_rb['def_Ry'] = def_yr_rb.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().sum())
    def_yr_rb['def_Ry_pg'] = def_yr_rb.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().mean())
    def_yr_rb['def_Rya'] = def_yr_rb['def_Ry'] / def_yr_rb['def_Ra']
    def_yr_rb['def_TDrush'] = def_yr_rb.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().sum())
    def_yr_rb['def_Rtd_pg'] = def_yr_rb.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().mean())
    
    def_yr_rb['def_Rec'] = def_yr_rb.groupby(['Opponent'])['Receptions'].transform(lambda x: x.shift().sum())
    def_yr_rb['def_Rec_pg'] = def_yr_rb.groupby(['Opponent'])['Receptions'].transform(lambda x: x.shift().mean())
    def_yr_rb['def_Tgt'] = def_yr_rb.groupby(['Opponent'])['ReceivingTargets'].transform(lambda x: x.shift().sum())
    def_yr_rb['def_Tgt_pg'] = def_yr_rb.groupby(['Opponent'])['ReceivingTargets'].transform(lambda x: x.shift().mean())
    def_yr_rb['def_Rec%'] =  def_yr_rb['def_Rec'] / def_yr_rb['def_Tgt']
    def_yr_rb['def_Recy'] = def_yr_rb.groupby(['Opponent'])['ReceivingYards'].transform(lambda x: x.shift().sum())
    def_yr_rb['def_Recy_pg'] = def_yr_rb.groupby(['Opponent'])['ReceivingYards'].transform(lambda x: x.shift().mean())
    def_yr_rb['def_TDrec'] = def_yr_rb.groupby(['Opponent'])['ReceivingTouchdowns'].transform(lambda x: x.shift().sum())
    def_yr_rb['def_Rectd_pg'] = def_yr_rb.groupby(['Opponent'])['ReceivingTouchdowns'].transform(lambda x: x.shift().mean())
    def_yr_rb['def_YdsPerTgt'] = def_yr_rb['def_Recy'] / def_yr_rb['def_Tgt']
    def_yr_rb['def_YdsPerRec'] = def_yr_rb['def_Recy'] / def_yr_rb['def_Rec']
    
    def_yr_rb['def_FumLost'] = def_yr_rb.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().sum())
    def_yr_rb['def_Fum_pg'] = def_yr_rb.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().mean())
    def_yr_rb = def_yr_rb.fillna(0)

    def_yr_rb['def_Rb_RaRank'] = def_yr_rb.groupby(['Week'])['def_Ra_pg'].rank(method='min')
    def_yr_rb['def_Rb_RyRank'] = def_yr_rb.groupby(['Week'])['def_Ry_pg'].rank(method='min')
    def_yr_rb['def_Rb_RyaRank'] = def_yr_rb.groupby(['Week'])['def_Rya'].rank(method='min')
    def_yr_rb['def_Rb_TDrushRank'] = def_yr_rb.groupby(['Week'])['def_Rtd_pg'].rank(method='min')
    def_yr_rb['def_Rb_RecRank'] = def_yr_rb.groupby(['Week'])['def_Rec_pg'].rank(method='min')
    def_yr_rb['def_Rb_TgtRank'] = def_yr_rb.groupby(['Week'])['def_Tgt_pg'].rank(method='min')
    def_yr_rb['def_Rb_RecyRank'] = def_yr_rb.groupby(['Week'])['def_Recy_pg'].rank(method='min')
    def_yr_rb['def_Rb_Rec%Rank'] = def_yr_rb.groupby(['Week'])['def_Rec%'].rank(method='min')
    def_yr_rb['def_Rb_TDrecRank'] = def_yr_rb.groupby(['Week'])['def_Rectd_pg'].rank(method='min')
    def_yr_rb['def_Rb_YdsPerTgtRank'] = def_yr_rb.groupby(['Week'])['def_YdsPerTgt'].rank(method='min')
    def_yr_rb['def_Rb_YdsPerRecRank'] = def_yr_rb.groupby(['Week'])['def_YdsPerRec'].rank(method='min')
    def_yr_rb['def_Rb_FumRank'] = def_yr_rb.groupby(['Week'])['def_Fum_pg'].rank(method='min')        
    def_yr_rb['def_Rb_DKPts'] = (def_yr_rb['def_Ry'] * .1 + def_yr_rb['def_TDrush'] * 6 +\
                         def_yr_rb['def_Rec'] * 1 + def_yr_rb['def_TDrec'] * 6 +\
                         def_yr_rb['def_Recy'] * .1 + def_yr_rb['def_FumLost'] * -1)        
    def_yr_rb['def_Rb_DKPtsRank'] = def_yr_rb.groupby(['Week'])['def_Rb_DKPts'].rank(method='min', ascending = False)
    def_rb_yr_dfs.append(def_yr_rb)
    
def_rb_stats = pd.concat(def_rb_yr_dfs).drop_duplicates().reset_index(drop=True)
def_rb_stats.to_csv(etl_dir + 'def_rb_stats.csv')


""" OFFENSE WIDE RECEIVERS """
wr_yr_dfs = []
for yr in range(2002, 2021): 
    yr_wr = player_stats[(player_stats['Season']==yr) & (player_stats['Position']=='WR')].copy().reset_index(drop=True)
    yr_wr.sort_values(['PlayerID', 'Week'], ascending = [True, True], inplace = True)
    
    yr_wr['Games3'] = yr_wr.groupby(['PlayerID'])['Played'].transform(lambda x: x.shift().rolling(3).sum())
    yr_wr['Ra3'] = yr_wr.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).sum())
    yr_wr['Ra_pg3'] = yr_wr.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).mean())
    yr_wr['Ry3'] = yr_wr.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().rolling(3).sum())
    yr_wr['Ry_pg3'] = yr_wr.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().rolling(3).mean())
    yr_wr['Rya3'] = yr_wr['Ry3'] / yr_wr['Ra3']
    yr_wr['TDrush3'] = yr_wr.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    yr_wr['Rtd_pg3'] = yr_wr.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    
    yr_wr['Rec3'] = yr_wr.groupby(['PlayerID'])['Receptions'].transform(lambda x: x.shift().rolling(3).sum())
    yr_wr['Rec_pg3'] = yr_wr.groupby(['PlayerID'])['Receptions'].transform(lambda x: x.shift().rolling(3).mean())
    yr_wr['Tgt3'] = yr_wr.groupby(['PlayerID'])['ReceivingTargets'].transform(lambda x: x.shift().rolling(3).sum())
    yr_wr['Tgt_pg3'] = yr_wr.groupby(['PlayerID'])['ReceivingTargets'].transform(lambda x: x.shift().rolling(3).mean())
    yr_wr['Rec%3'] =  yr_wr['Rec3'] / yr_wr['Tgt3']
    yr_wr['Recy3'] = yr_wr.groupby(['PlayerID'])['ReceivingYards'].transform(lambda x: x.shift().rolling(3).sum())
    yr_wr['Recy_pg3'] = yr_wr.groupby(['PlayerID'])['ReceivingYards'].transform(lambda x: x.shift().rolling(3).mean())
    yr_wr['TDrec3'] = yr_wr.groupby(['PlayerID'])['ReceivingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    yr_wr['Rectd_pg3'] = yr_wr.groupby(['PlayerID'])['ReceivingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    yr_wr['YdsPerTgt3'] = yr_wr['Recy3'] / yr_wr['Tgt3']
    yr_wr['YdsPerRec3'] = yr_wr['Recy3'] / yr_wr['Rec3']
    
    yr_wr['FumLost3'] = yr_wr.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).sum())
    yr_wr['Fum_pg3'] = yr_wr.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).mean())
    yr_wr = yr_wr.fillna(0)

    yr_wr['Wr_RaRank3'] = yr_wr.groupby(['Week'])['Ra_pg3'].rank(method='min')
    yr_wr['Wr_RyRank3'] = yr_wr.groupby(['Week'])['Ry_pg3'].rank(method='min')
    yr_wr['Wr_RyaRank3'] = yr_wr.groupby(['Week'])['Rya3'].rank(method='min')
    yr_wr['Wr_TDrushRank3'] = yr_wr.groupby(['Week'])['Rtd_pg3'].rank(method='min')
    yr_wr['Wr_RecRank3'] = yr_wr.groupby(['Week'])['Rec_pg3'].rank(method='min')
    yr_wr['Wr_TgtRank3'] = yr_wr.groupby(['Week'])['Tgt_pg3'].rank(method='min')
    yr_wr['Wr_RecyRank3'] = yr_wr.groupby(['Week'])['Recy_pg3'].rank(method='min')
    yr_wr['Wr_Rec%Rank3'] = yr_wr.groupby(['Week'])['Rec%3'].rank(method='min')
    yr_wr['Wr_TDrecRank3'] = yr_wr.groupby(['Week'])['Rectd_pg3'].rank(method='min')
    yr_wr['Wr_YdsPerTgtRank3'] = yr_wr.groupby(['Week'])['YdsPerTgt3'].rank(method='min')
    yr_wr['Wr_YdsPerRecRank3'] = yr_wr.groupby(['Week'])['YdsPerRec3'].rank(method='min')
    yr_wr['Wr_FumRank3'] = yr_wr.groupby(['Week'])['Fum_pg3'].rank(method='min')     
    
    yr_wr['Wr_DKPts3'] =  (yr_wr['Ry3'] * .1 + yr_wr['TDrush3'] * 6 +\
                           yr_wr['Rec3'] * 1 + yr_wr['TDrec3'] * 6 +\
                           yr_wr['Recy3'] * .1 + yr_wr['FumLost3'] * -1) / yr_wr['Games3']
                              
    yr_wr['Wr_DKPtsRank3'] = yr_wr.groupby(['Week'])['Wr_DKPts3'].rank(method='min', ascending = False)
    
    yr_wr['Games'] = yr_wr.groupby(['PlayerID'])['Played'].transform(lambda x: x.shift().sum())
    yr_wr['Ra'] = yr_wr.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().sum())
    yr_wr['Ra_pg'] = yr_wr.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().mean())
    yr_wr['Ry'] = yr_wr.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().sum())
    yr_wr['Ry_pg'] = yr_wr.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().mean())
    yr_wr['Rya'] = yr_wr['Ry'] / yr_wr['Ra']
    yr_wr['TDrush'] = yr_wr.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().sum())
    yr_wr['Rtd_pg'] = yr_wr.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().mean())
    
    yr_wr['Rec'] = yr_wr.groupby(['PlayerID'])['Receptions'].transform(lambda x: x.shift().sum())
    yr_wr['Rec_pg'] = yr_wr.groupby(['PlayerID'])['Receptions'].transform(lambda x: x.shift().mean())
    yr_wr['Tgt'] = yr_wr.groupby(['PlayerID'])['ReceivingTargets'].transform(lambda x: x.shift().sum())
    yr_wr['Tgt_pg'] = yr_wr.groupby(['PlayerID'])['ReceivingTargets'].transform(lambda x: x.shift().mean())
    yr_wr['Rec%'] =  yr_wr['Rec'] / yr_wr['Tgt']
    yr_wr['Recy'] = yr_wr.groupby(['PlayerID'])['ReceivingYards'].transform(lambda x: x.shift().sum())
    yr_wr['Recy_pg'] = yr_wr.groupby(['PlayerID'])['ReceivingYards'].transform(lambda x: x.shift().mean())
    yr_wr['TDrec'] = yr_wr.groupby(['PlayerID'])['ReceivingTouchdowns'].transform(lambda x: x.shift().sum())
    yr_wr['Rectd_pg'] = yr_wr.groupby(['PlayerID'])['ReceivingTouchdowns'].transform(lambda x: x.shift().mean())
    yr_wr['YdsPerTgt'] = yr_wr['Recy'] / yr_wr['Tgt']
    yr_wr['YdsPerRec'] = yr_wr['Recy'] / yr_wr['Rec']
    
    yr_wr['FumLost'] = yr_wr.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().sum())
    yr_wr['Fum_pg'] = yr_wr.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().mean())
    yr_wr = yr_wr.fillna(0)

    yr_wr['Wr_RaRank'] = yr_wr.groupby(['Week'])['Ra_pg'].rank(method='min')
    yr_wr['Wr_RyRank'] = yr_wr.groupby(['Week'])['Ry_pg'].rank(method='min')
    yr_wr['Wr_RyaRank'] = yr_wr.groupby(['Week'])['Rya'].rank(method='min')
    yr_wr['Wr_TDrushRank'] = yr_wr.groupby(['Week'])['Rtd_pg'].rank(method='min')
    yr_wr['Wr_RecRank'] = yr_wr.groupby(['Week'])['Rec_pg'].rank(method='min')
    yr_wr['Wr_TgtRank'] = yr_wr.groupby(['Week'])['Tgt_pg'].rank(method='min')
    yr_wr['Wr_RecyRank'] = yr_wr.groupby(['Week'])['Recy_pg'].rank(method='min')
    yr_wr['Wr_Rec%Rank'] = yr_wr.groupby(['Week'])['Rec%'].rank(method='min')
    yr_wr['Wr_TDrecRank'] = yr_wr.groupby(['Week'])['Rectd_pg'].rank(method='min')
    yr_wr['Wr_YdsPerTgtRank'] = yr_wr.groupby(['Week'])['YdsPerTgt'].rank(method='min')
    yr_wr['Wr_YdsPerRecRank'] = yr_wr.groupby(['Week'])['YdsPerRec'].rank(method='min')
    yr_wr['Wr_FumRank'] = yr_wr.groupby(['Week'])['Fum_pg'].rank(method='min')        
    yr_wr['Wr_DKPts'] = (yr_wr['Ry'] * .1 + yr_wr['TDrush'] * 6 +\
                         yr_wr['Rec'] * 1 + yr_wr['TDrec'] * 6 +\
                         yr_wr['Recy'] * .1 + yr_wr['FumLost'] * -1)        
    yr_wr['Wr_DKPtsRank'] = yr_wr.groupby(['Week'])['Wr_DKPts'].rank(method='min', ascending = False)
    
    wr_yr_dfs.append(yr_wr)

wr_stats = pd.concat(rb_yr_dfs).drop_duplicates().reset_index(drop=True)
wr_stats_all = pd.merge(wr_stats, player_stats[['Season','PlayerID','Team','Name']].drop_duplicates(), how = 'left', on = ['Season','PlayerID'])
wr_stats_all.to_csv(etl_dir + 'wr_stats.csv')


""" DEFENSE WIDE RECEIVERS """
def_wr_yr_dfs = []
for yr in range(2002, 2021): 
    def_yr_wr = player_stats[(player_stats['Season']==yr) & (player_stats['Position']=='WR')].copy().reset_index(drop=True)
    def_yr_wr.sort_values(['PlayerID', 'Week'], ascending = [True, True], inplace = True)
    
    def_yr_wr['def_Games3'] = def_yr_wr.groupby(['Opponent'])['Played'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_wr['def_Ra3'] = def_yr_wr.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_wr['def_Ra_pg3'] = def_yr_wr.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_wr['def_Ry3'] = def_yr_wr.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_wr['def_Ry_pg3'] = def_yr_wr.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_wr['def_Rya3'] = def_yr_wr['def_Ry3'] / def_yr_wr['def_Ra3']
    def_yr_wr['def_TDrush3'] = def_yr_wr.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_wr['def_Rtd_pg3'] = def_yr_wr.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    
    def_yr_wr['def_Rec3'] = def_yr_wr.groupby(['Opponent'])['Receptions'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_wr['def_Rec_pg3'] = def_yr_wr.groupby(['Opponent'])['Receptions'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_wr['def_Tgt3'] = def_yr_wr.groupby(['Opponent'])['ReceivingTargets'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_wr['def_Tgt_pg3'] = def_yr_wr.groupby(['Opponent'])['ReceivingTargets'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_wr['def_Rec%3'] =  def_yr_wr['def_Rec3'] / def_yr_wr['def_Tgt3']
    def_yr_wr['def_Recy3'] = def_yr_wr.groupby(['Opponent'])['ReceivingYards'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_wr['def_Recy_pg3'] = def_yr_wr.groupby(['Opponent'])['ReceivingYards'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_wr['def_TDrec3'] = def_yr_wr.groupby(['Opponent'])['ReceivingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_wr['def_Rectd_pg3'] = def_yr_wr.groupby(['Opponent'])['ReceivingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_wr['def_YdsPerTgt3'] = def_yr_wr['def_Recy3'] / def_yr_wr['def_Tgt3']
    def_yr_wr['def_YdsPerRec3'] = def_yr_wr['def_Recy3'] / def_yr_wr['def_Rec3']
    
    def_yr_wr['def_FumLost3'] = def_yr_wr.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_wr['def_Fum_pg3'] = def_yr_wr.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_wr = def_yr_wr.fillna(0)

    def_yr_wr['def_Wr_RaRank3'] = def_yr_wr.groupby(['Week'])['def_Ra_pg3'].rank(method='min')
    def_yr_wr['def_Wr_RyRank3'] = def_yr_wr.groupby(['Week'])['def_Ry_pg3'].rank(method='min')
    def_yr_wr['def_Wr_RyaRank3'] = def_yr_wr.groupby(['Week'])['def_Rya3'].rank(method='min')
    def_yr_wr['def_Wr_TDrushRank3'] = def_yr_wr.groupby(['Week'])['def_Rtd_pg3'].rank(method='min')
    def_yr_wr['def_Wr_RecRank3'] = def_yr_wr.groupby(['Week'])['def_Rec_pg3'].rank(method='min')
    def_yr_wr['def_Wr_TgtRank3'] = def_yr_wr.groupby(['Week'])['def_Tgt_pg3'].rank(method='min')
    def_yr_wr['def_Wr_RecyRank3'] = def_yr_wr.groupby(['Week'])['def_Recy_pg3'].rank(method='min')
    def_yr_wr['def_Wr_Rec%Rank3'] = def_yr_wr.groupby(['Week'])['def_Rec%3'].rank(method='min')
    def_yr_wr['def_Wr_TDrecRank3'] = def_yr_wr.groupby(['Week'])['def_Rectd_pg3'].rank(method='min')
    def_yr_wr['def_Wr_YdsPerTgtRank3'] = def_yr_wr.groupby(['Week'])['def_YdsPerTgt3'].rank(method='min')
    def_yr_wr['def_Wr_YdsPerRecRank3'] = def_yr_wr.groupby(['Week'])['def_YdsPerRec3'].rank(method='min')
    def_yr_wr['def_Wr_FumRank3'] = def_yr_wr.groupby(['Week'])['def_Fum_pg3'].rank(method='min')   
    
    def_yr_wr['def_Wr_DKPts3'] =  (def_yr_wr['def_Ry3'] * .1 + def_yr_wr['def_TDrush3'] * 6 +\
                           def_yr_wr['def_Rec3'] * 1 + def_yr_wr['def_TDrec3'] * 6 +\
                           def_yr_wr['def_Recy3'] * .1 + def_yr_wr['def_FumLost3'] * -1) / def_yr_wr['def_Games3'] 
                              
    def_yr_wr['def_Wr_DKPtsRank3'] = def_yr_wr.groupby(['Week'])['def_Wr_DKPts3'].rank(method='min', ascending = False)
    
    def_yr_wr['def_Games'] = def_yr_wr.groupby(['Opponent'])['Played'].transform(lambda x: x.shift().sum())
    def_yr_wr['def_Ra'] = def_yr_wr.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().sum())
    def_yr_wr['def_Ra_pg'] = def_yr_wr.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().mean())
    def_yr_wr['def_Ry'] = def_yr_wr.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().sum())
    def_yr_wr['def_Ry_pg'] = def_yr_wr.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().mean())
    def_yr_wr['def_Rya'] = def_yr_wr['def_Ry'] / def_yr_wr['def_Ra']
    def_yr_wr['def_TDrush'] = def_yr_wr.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().sum())
    def_yr_wr['def_Rtd_pg'] = def_yr_wr.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().mean())
    
    def_yr_wr['def_Rec'] = def_yr_wr.groupby(['Opponent'])['Receptions'].transform(lambda x: x.shift().sum())
    def_yr_wr['def_Rec_pg'] = def_yr_wr.groupby(['Opponent'])['Receptions'].transform(lambda x: x.shift().mean())
    def_yr_wr['def_Tgt'] = def_yr_wr.groupby(['Opponent'])['ReceivingTargets'].transform(lambda x: x.shift().sum())
    def_yr_wr['def_Tgt_pg'] = def_yr_wr.groupby(['Opponent'])['ReceivingTargets'].transform(lambda x: x.shift().mean())
    def_yr_wr['def_Rec%'] =  def_yr_wr['def_Rec'] / def_yr_wr['def_Tgt']
    def_yr_wr['def_Recy'] = def_yr_wr.groupby(['Opponent'])['ReceivingYards'].transform(lambda x: x.shift().sum())
    def_yr_wr['def_Recy_pg'] = def_yr_wr.groupby(['Opponent'])['ReceivingYards'].transform(lambda x: x.shift().mean())
    def_yr_wr['def_TDrec'] = def_yr_wr.groupby(['Opponent'])['ReceivingTouchdowns'].transform(lambda x: x.shift().sum())
    def_yr_wr['def_Rectd_pg'] = def_yr_wr.groupby(['Opponent'])['ReceivingTouchdowns'].transform(lambda x: x.shift().mean())
    def_yr_wr['def_YdsPerTgt'] = def_yr_wr['def_Recy'] / def_yr_wr['def_Tgt']
    def_yr_wr['def_YdsPerRec'] = def_yr_wr['def_Recy'] / def_yr_wr['def_Rec']
    
    def_yr_wr['def_FumLost'] = def_yr_wr.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().sum())
    def_yr_wr['def_Fum_pg'] = def_yr_wr.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().mean())
    def_yr_wr = def_yr_wr.fillna(0)

    def_yr_wr['def_Wr_RaRank'] = def_yr_wr.groupby(['Week'])['def_Ra_pg'].rank(method='min')
    def_yr_wr['def_Wr_RyRank'] = def_yr_wr.groupby(['Week'])['def_Ry_pg'].rank(method='min')
    def_yr_wr['def_Wr_RyaRank'] = def_yr_wr.groupby(['Week'])['def_Rya'].rank(method='min')
    def_yr_wr['def_Wr_TDrushRank'] = def_yr_wr.groupby(['Week'])['def_Rtd_pg'].rank(method='min')
    def_yr_wr['def_Wr_RecRank'] = def_yr_wr.groupby(['Week'])['def_Rec_pg'].rank(method='min')
    def_yr_wr['def_Wr_TgtRank'] = def_yr_wr.groupby(['Week'])['def_Tgt_pg'].rank(method='min')
    def_yr_wr['def_Wr_RecyRank'] = def_yr_wr.groupby(['Week'])['def_Recy_pg'].rank(method='min')
    def_yr_wr['def_Wr_Rec%Rank'] = def_yr_wr.groupby(['Week'])['def_Rec%'].rank(method='min')
    def_yr_wr['def_Wr_TDrecRank'] = def_yr_wr.groupby(['Week'])['def_Rectd_pg'].rank(method='min')
    def_yr_wr['def_Wr_YdsPerTgtRank'] = def_yr_wr.groupby(['Week'])['def_YdsPerTgt'].rank(method='min')
    def_yr_wr['def_Wr_YdsPerRecRank'] = def_yr_wr.groupby(['Week'])['def_YdsPerRec'].rank(method='min')
    def_yr_wr['def_Wr_FumRank'] = def_yr_wr.groupby(['Week'])['def_Fum_pg'].rank(method='min')        
    def_yr_wr['def_Wr_DKPts'] = (def_yr_wr['def_Ry'] * .1 + def_yr_wr['def_TDrush'] * 6 +\
                         def_yr_wr['def_Rec'] * 1 + def_yr_wr['def_TDrec'] * 6 +\
                         def_yr_wr['def_Recy'] * .1 + def_yr_wr['def_FumLost'] * -1)        
    def_yr_wr['def_Wr_DKPtsRank'] = def_yr_wr.groupby(['Week'])['def_Wr_DKPts'].rank(method='min', ascending = False)
    def_wr_yr_dfs.append(def_yr_wr)
    
def_wr_stats = pd.concat(def_wr_yr_dfs).drop_duplicates().reset_index(drop=True)
def_wr_stats.to_csv(etl_dir + 'def_wr_stats.csv')




""" OFFENSE TIGHT ENDS """
te_yr_dfs = []
for yr in range(2002, 2021): 
    yr_te = player_stats[(player_stats['Season']==yr) & (player_stats['Position']=='TE')].copy().reset_index(drop=True)
    yr_te.sort_values(['PlayerID', 'Week'], ascending = [True, True], inplace = True)
    
    yr_te['Games3'] = yr_te.groupby(['PlayerID'])['Played'].transform(lambda x: x.shift().rolling(3).sum())
    yr_te['Ra3'] = yr_te.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).sum())
    yr_te['Ra_pg3'] = yr_te.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).mean())
    yr_te['Ry3'] = yr_te.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().rolling(3).sum())
    yr_te['Ry_pg3'] = yr_te.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().rolling(3).mean())
    yr_te['Rya3'] = yr_te['Ry3'] / yr_te['Ra3']
    yr_te['TDrush3'] = yr_te.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    yr_te['Rtd_pg3'] = yr_te.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    
    yr_te['Rec3'] = yr_te.groupby(['PlayerID'])['Receptions'].transform(lambda x: x.shift().rolling(3).sum())
    yr_te['Rec_pg3'] = yr_te.groupby(['PlayerID'])['Receptions'].transform(lambda x: x.shift().rolling(3).mean())
    yr_te['Tgt3'] = yr_te.groupby(['PlayerID'])['ReceivingTargets'].transform(lambda x: x.shift().rolling(3).sum())
    yr_te['Tgt_pg3'] = yr_te.groupby(['PlayerID'])['ReceivingTargets'].transform(lambda x: x.shift().rolling(3).mean())
    yr_te['Rec%3'] =  yr_te['Rec3'] / yr_te['Tgt3']
    yr_te['Recy3'] = yr_te.groupby(['PlayerID'])['ReceivingYards'].transform(lambda x: x.shift().rolling(3).sum())
    yr_te['Recy_pg3'] = yr_te.groupby(['PlayerID'])['ReceivingYards'].transform(lambda x: x.shift().rolling(3).mean())
    yr_te['TDrec3'] = yr_te.groupby(['PlayerID'])['ReceivingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    yr_te['Rectd_pg3'] = yr_te.groupby(['PlayerID'])['ReceivingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    yr_te['YdsPerTgt3'] = yr_te['Recy3'] / yr_te['Tgt3']
    yr_te['YdsPerRec3'] = yr_te['Recy3'] / yr_te['Rec3']
    
    yr_te['FumLost3'] = yr_te.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).sum())
    yr_te['Fum_pg3'] = yr_te.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).mean())
    yr_te = yr_te.fillna(0)

    yr_te['Te_RaRank3'] = yr_te.groupby(['Week'])['Ra_pg3'].rank(method='min')
    yr_te['Te_RyRank3'] = yr_te.groupby(['Week'])['Ry_pg3'].rank(method='min')
    yr_te['Te_RyaRank3'] = yr_te.groupby(['Week'])['Rya3'].rank(method='min')
    yr_te['Te_TDrushRank3'] = yr_te.groupby(['Week'])['Rtd_pg3'].rank(method='min')
    yr_te['Te_RecRank3'] = yr_te.groupby(['Week'])['Rec_pg3'].rank(method='min')
    yr_te['Te_TgtRank3'] = yr_te.groupby(['Week'])['Tgt_pg3'].rank(method='min')
    yr_te['Te_RecyRank3'] = yr_te.groupby(['Week'])['Recy_pg3'].rank(method='min')
    yr_te['Te_Rec%Rank3'] = yr_te.groupby(['Week'])['Rec%3'].rank(method='min')
    yr_te['Te_TDrecRank3'] = yr_te.groupby(['Week'])['Rectd_pg3'].rank(method='min')
    yr_te['Te_YdsPerTgtRank3'] = yr_te.groupby(['Week'])['YdsPerTgt3'].rank(method='min')
    yr_te['Te_YdsPerRecRank3'] = yr_te.groupby(['Week'])['YdsPerRec3'].rank(method='min')
    yr_te['Te_FumRank3'] = yr_te.groupby(['Week'])['Fum_pg3'].rank(method='min')        
    yr_te['Te_DKPts3'] =  (yr_te['Ry3'] * .1 + yr_te['TDrush3'] * 6 +\
                           yr_te['Rec3'] * 1 + yr_te['TDrec3'] * 6 +\
                           yr_te['Recy3'] * .1 + yr_te['FumLost3'] * -1) / yr_te['Games3']
                              
    yr_te['Te_DKPtsRank3'] = yr_te.groupby(['Week'])['Te_DKPts3'].rank(method='min', ascending = False)
    
    yr_te['Games'] = yr_te.groupby(['PlayerID'])['Played'].transform(lambda x: x.shift().sum())
    yr_te['Ra'] = yr_te.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().sum())
    yr_te['Ra_pg'] = yr_te.groupby(['PlayerID'])['RushingAttempts'].transform(lambda x: x.shift().mean())
    yr_te['Ry'] = yr_te.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().sum())
    yr_te['Ry_pg'] = yr_te.groupby(['PlayerID'])['RushingYards'].transform(lambda x: x.shift().mean())
    yr_te['Rya'] = yr_te['Ry'] / yr_te['Ra']
    yr_te['TDrush'] = yr_te.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().sum())
    yr_te['Rtd_pg'] = yr_te.groupby(['PlayerID'])['RushingTouchdowns'].transform(lambda x: x.shift().mean())
    
    yr_te['Rec'] = yr_te.groupby(['PlayerID'])['Receptions'].transform(lambda x: x.shift().sum())
    yr_te['Rec_pg'] = yr_te.groupby(['PlayerID'])['Receptions'].transform(lambda x: x.shift().mean())
    yr_te['Tgt'] = yr_te.groupby(['PlayerID'])['ReceivingTargets'].transform(lambda x: x.shift().sum())
    yr_te['Tgt_pg'] = yr_te.groupby(['PlayerID'])['ReceivingTargets'].transform(lambda x: x.shift().mean())
    yr_te['Rec%'] =  yr_te['Rec'] / yr_te['Tgt']
    yr_te['Recy'] = yr_te.groupby(['PlayerID'])['ReceivingYards'].transform(lambda x: x.shift().sum())
    yr_te['Recy_pg'] = yr_te.groupby(['PlayerID'])['ReceivingYards'].transform(lambda x: x.shift().mean())
    yr_te['TDrec'] = yr_te.groupby(['PlayerID'])['ReceivingTouchdowns'].transform(lambda x: x.shift().sum())
    yr_te['Rectd_pg'] = yr_te.groupby(['PlayerID'])['ReceivingTouchdowns'].transform(lambda x: x.shift().mean())
    yr_te['YdsPerTgt'] = yr_te['Recy'] / yr_te['Tgt']
    yr_te['YdsPerRec'] = yr_te['Recy'] / yr_te['Rec']
    
    yr_te['FumLost'] = yr_te.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().sum())
    yr_te['Fum_pg'] = yr_te.groupby(['PlayerID'])['FumblesLost'].transform(lambda x: x.shift().mean())
    yr_te = yr_te.fillna(0)

    yr_te['Te_RaRank'] = yr_te.groupby(['Week'])['Ra_pg'].rank(method='min')
    yr_te['Te_RyRank'] = yr_te.groupby(['Week'])['Ry_pg'].rank(method='min')
    yr_te['Te_RyaRank'] = yr_te.groupby(['Week'])['Rya'].rank(method='min')
    yr_te['Te_TDrushRank'] = yr_te.groupby(['Week'])['Rtd_pg'].rank(method='min')
    yr_te['Te_RecRank'] = yr_te.groupby(['Week'])['Rec_pg'].rank(method='min')
    yr_te['Te_TgtRank'] = yr_te.groupby(['Week'])['Tgt_pg'].rank(method='min')
    yr_te['Te_RecyRank'] = yr_te.groupby(['Week'])['Recy_pg'].rank(method='min')
    yr_te['Te_Rec%Rank'] = yr_te.groupby(['Week'])['Rec%'].rank(method='min')
    yr_te['Te_TDrecRank'] = yr_te.groupby(['Week'])['Rectd_pg'].rank(method='min')
    yr_te['Te_YdsPerTgtRank'] = yr_te.groupby(['Week'])['YdsPerTgt'].rank(method='min')
    yr_te['Te_YdsPerRecRank'] = yr_te.groupby(['Week'])['YdsPerRec'].rank(method='min')
    yr_te['Te_FumRank'] = yr_te.groupby(['Week'])['Fum_pg'].rank(method='min')        
    yr_te['Te_DKPts'] = (yr_te['Ry'] * .1 + yr_te['TDrush'] * 6 +\
                         yr_te['Rec'] * 1 + yr_te['TDrec'] * 6 +\
                         yr_te['Recy'] * .1 + yr_te['FumLost'] * -1)        
    yr_te['Te_DKPtsRank'] = yr_te.groupby(['Week'])['Te_DKPts'].rank(method='min', ascending = False)
    
    te_yr_dfs.append(yr_te)

te_stats = pd.concat(rb_yr_dfs).drop_duplicates().reset_index(drop=True)
te_stats_all = pd.merge(te_stats, player_stats[['Season','PlayerID','Team','Name']].drop_duplicates(), how = 'left', on = ['Season','PlayerID'])
te_stats_all.to_csv(etl_dir + 'te_stats.csv')



""" DEFENSE TIGHT ENDS """
def_te_yr_dfs = []
for yr in range(2002, 2021): 
    def_yr_te = player_stats[(player_stats['Season']==yr) & (player_stats['Position']=='TE')].copy().reset_index(drop=True)
    def_yr_te.sort_values(['PlayerID', 'Week'], ascending = [True, True], inplace = True)
    
    def_yr_te['def_Games3'] = def_yr_te.groupby(['Opponent'])['Played'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_te['def_Ra3'] = def_yr_te.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_te['def_Ra_pg3'] = def_yr_te.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_te['def_Ry3'] = def_yr_te.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_te['def_Ry_pg3'] = def_yr_te.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_te['def_Rya3'] = def_yr_te['def_Ry3'] / def_yr_te['def_Ra3']
    def_yr_te['def_TDrush3'] = def_yr_te.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_te['def_Rtd_pg3'] = def_yr_te.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    
    def_yr_te['def_Rec3'] = def_yr_te.groupby(['Opponent'])['Receptions'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_te['def_Rec_pg3'] = def_yr_te.groupby(['Opponent'])['Receptions'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_te['def_Tgt3'] = def_yr_te.groupby(['Opponent'])['ReceivingTargets'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_te['def_Tgt_pg3'] = def_yr_te.groupby(['Opponent'])['ReceivingTargets'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_te['def_Rec%3'] =  def_yr_te['def_Rec3'] / def_yr_te['def_Tgt3']
    def_yr_te['def_Recy3'] = def_yr_te.groupby(['Opponent'])['ReceivingYards'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_te['def_Recy_pg3'] = def_yr_te.groupby(['Opponent'])['ReceivingYards'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_te['def_TDrec3'] = def_yr_te.groupby(['Opponent'])['ReceivingTouchdowns'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_te['def_Rectd_pg3'] = def_yr_te.groupby(['Opponent'])['ReceivingTouchdowns'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_te['def_YdsPerTgt3'] = def_yr_te['def_Recy3'] / def_yr_te['def_Tgt3']
    def_yr_te['def_YdsPerRec3'] = def_yr_te['def_Recy3'] / def_yr_te['def_Rec3']
    
    def_yr_te['def_FumLost3'] = def_yr_te.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).sum())
    def_yr_te['def_Fum_pg3'] = def_yr_te.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().rolling(3).mean())
    def_yr_te = def_yr_te.fillna(0)

    def_yr_te['def_Te_RaRank3'] = def_yr_te.groupby(['Week'])['def_Ra_pg3'].rank(method='min')
    def_yr_te['def_Te_RyRank3'] = def_yr_te.groupby(['Week'])['def_Ry_pg3'].rank(method='min')
    def_yr_te['def_Te_RyaRank3'] = def_yr_te.groupby(['Week'])['def_Rya3'].rank(method='min')
    def_yr_te['def_Te_TDrushRank3'] = def_yr_te.groupby(['Week'])['def_Rtd_pg3'].rank(method='min')
    def_yr_te['def_Te_RecRank3'] = def_yr_te.groupby(['Week'])['def_Rec_pg3'].rank(method='min')
    def_yr_te['def_Te_TgtRank3'] = def_yr_te.groupby(['Week'])['def_Tgt_pg3'].rank(method='min')
    def_yr_te['def_Te_RecyRank3'] = def_yr_te.groupby(['Week'])['def_Recy_pg3'].rank(method='min')
    def_yr_te['def_Te_Rec%Rank3'] = def_yr_te.groupby(['Week'])['def_Rec%3'].rank(method='min')
    def_yr_te['def_Te_TDrecRank3'] = def_yr_te.groupby(['Week'])['def_Rectd_pg3'].rank(method='min')
    def_yr_te['def_Te_YdsPerTgtRank3'] = def_yr_te.groupby(['Week'])['def_YdsPerTgt3'].rank(method='min')
    def_yr_te['def_Te_YdsPerRecRank3'] = def_yr_te.groupby(['Week'])['def_YdsPerRec3'].rank(method='min')
    def_yr_te['def_Te_FumRank3'] = def_yr_te.groupby(['Week'])['def_Fum_pg3'].rank(method='min')   
     
    def_yr_te['def_Te_DKPts3'] =  (def_yr_te['def_Ry3'] * .1 + def_yr_te['def_TDrush3'] * 6 +\
                           def_yr_te['def_Rec3'] * 1 + def_yr_te['def_TDrec3'] * 6 +\
                           def_yr_te['def_Recy3'] * .1 + def_yr_te['def_FumLost3'] * -1) / def_yr_te['def_Games3'] 
                              
    def_yr_te['def_Te_DKPtsRank3'] = def_yr_te.groupby(['Week'])['def_Te_DKPts3'].rank(method='min', ascending = False)
    
    def_yr_te['def_Games'] = def_yr_te.groupby(['Opponent'])['Played'].transform(lambda x: x.shift().sum())
    def_yr_te['def_Ra'] = def_yr_te.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().sum())
    def_yr_te['def_Ra_pg'] = def_yr_te.groupby(['Opponent'])['RushingAttempts'].transform(lambda x: x.shift().mean())
    def_yr_te['def_Ry'] = def_yr_te.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().sum())
    def_yr_te['def_Ry_pg'] = def_yr_te.groupby(['Opponent'])['RushingYards'].transform(lambda x: x.shift().mean())
    def_yr_te['def_Rya'] = def_yr_te['def_Ry'] / def_yr_te['def_Ra']
    def_yr_te['def_TDrush'] = def_yr_te.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().sum())
    def_yr_te['def_Rtd_pg'] = def_yr_te.groupby(['Opponent'])['RushingTouchdowns'].transform(lambda x: x.shift().mean())
    
    def_yr_te['def_Rec'] = def_yr_te.groupby(['Opponent'])['Receptions'].transform(lambda x: x.shift().sum())
    def_yr_te['def_Rec_pg'] = def_yr_te.groupby(['Opponent'])['Receptions'].transform(lambda x: x.shift().mean())
    def_yr_te['def_Tgt'] = def_yr_te.groupby(['Opponent'])['ReceivingTargets'].transform(lambda x: x.shift().sum())
    def_yr_te['def_Tgt_pg'] = def_yr_te.groupby(['Opponent'])['ReceivingTargets'].transform(lambda x: x.shift().mean())
    def_yr_te['def_Rec%'] =  def_yr_te['def_Rec'] / def_yr_te['def_Tgt']
    def_yr_te['def_Recy'] = def_yr_te.groupby(['Opponent'])['ReceivingYards'].transform(lambda x: x.shift().sum())
    def_yr_te['def_Recy_pg'] = def_yr_te.groupby(['Opponent'])['ReceivingYards'].transform(lambda x: x.shift().mean())
    def_yr_te['def_TDrec'] = def_yr_te.groupby(['Opponent'])['ReceivingTouchdowns'].transform(lambda x: x.shift().sum())
    def_yr_te['def_Rectd_pg'] = def_yr_te.groupby(['Opponent'])['ReceivingTouchdowns'].transform(lambda x: x.shift().mean())
    def_yr_te['def_YdsPerTgt'] = def_yr_te['def_Recy'] / def_yr_te['def_Tgt']
    def_yr_te['def_YdsPerRec'] = def_yr_te['def_Recy'] / def_yr_te['def_Rec']
    
    def_yr_te['def_FumLost'] = def_yr_te.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().sum())
    def_yr_te['def_Fum_pg'] = def_yr_te.groupby(['Opponent'])['FumblesLost'].transform(lambda x: x.shift().mean())
    def_yr_te = def_yr_te.fillna(0)

    def_yr_te['def_Te_RaRank'] = def_yr_te.groupby(['Week'])['def_Ra_pg'].rank(method='min')
    def_yr_te['def_Te_RyRank'] = def_yr_te.groupby(['Week'])['def_Ry_pg'].rank(method='min')
    def_yr_te['def_Te_RyaRank'] = def_yr_te.groupby(['Week'])['def_Rya'].rank(method='min')
    def_yr_te['def_Te_TDrushRank'] = def_yr_te.groupby(['Week'])['def_Rtd_pg'].rank(method='min')
    def_yr_te['def_Te_RecRank'] = def_yr_te.groupby(['Week'])['def_Rec_pg'].rank(method='min')
    def_yr_te['def_Te_TgtRank'] = def_yr_te.groupby(['Week'])['def_Tgt_pg'].rank(method='min')
    def_yr_te['def_Te_RecyRank'] = def_yr_te.groupby(['Week'])['def_Recy_pg'].rank(method='min')
    def_yr_te['def_Te_Rec%Rank'] = def_yr_te.groupby(['Week'])['def_Rec%'].rank(method='min')
    def_yr_te['def_Te_TDrecRank'] = def_yr_te.groupby(['Week'])['def_Rectd_pg'].rank(method='min')
    def_yr_te['def_Te_YdsPerTgtRank'] = def_yr_te.groupby(['Week'])['def_YdsPerTgt'].rank(method='min')
    def_yr_te['def_Te_YdsPerRecRank'] = def_yr_te.groupby(['Week'])['def_YdsPerRec'].rank(method='min')
    def_yr_te['def_Te_FumRank'] = def_yr_te.groupby(['Week'])['def_Fum_pg'].rank(method='min')        
    def_yr_te['def_Te_DKPts'] = (def_yr_te['def_Ry'] * .1 + def_yr_te['def_TDrush'] * 6 +\
                                 def_yr_te['def_Rec'] * 1 + def_yr_te['def_TDrec'] * 6 +\
                                 def_yr_te['def_Recy'] * .1 + def_yr_te['def_FumLost'] * -1)        
    def_yr_te['def_Te_DKPtsRank'] = def_yr_te.groupby(['Week'])['def_Te_DKPts'].rank(method='min', ascending = False)
    def_te_yr_dfs.append(def_yr_te)
    
def_te_stats = pd.concat(def_te_yr_dfs).drop_duplicates().reset_index(drop=True)
def_te_stats.to_csv(etl_dir + 'def_te_stats.csv')
