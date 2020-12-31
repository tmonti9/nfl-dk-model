# -*- coding: utf-8 -*-
"""
"""

import os
import pulp
import numpy as np
import pandas as pd

curr_wk = 16
""" YOU NEED TO UPDATE THE SCHEDULE FILE TO INCLUDE CURRENT WEEK GAMES """

working_directory = ''
pred_dir = ''
os.chdir(working_directory)

def pred_lineup(df, lineups, overlap):
    

    prob = pulp.LpProblem('NFLDK', pulp.LpMinimize)
    num_all = df.shape[0]
    salary_cap = 50000
    players_lineup = [pulp.LpVariable("Player_{}".format(i+1), cat="Binary") for i in range(num_all)]
    
    #add the objective
    prob += pulp.lpSum((pulp.lpSum(df.loc[i, 'Rank'] * players_lineup[i] for i in range(num_all))))
    

    prob += ((pulp.lpSum(df.loc[i, 'Pos_QB'] * players_lineup[i] for i in range(num_all)))  == 1)
    prob += ((pulp.lpSum(df.loc[i, 'Pos_RB'] * players_lineup[i] for i in range(num_all)))  >= 2)
    prob += ((pulp.lpSum(df.loc[i, 'Pos_RB'] * players_lineup[i] for i in range(num_all)))  <= 3)
    prob += ((pulp.lpSum(df.loc[i, 'Pos_WR'] * players_lineup[i] for i in range(num_all)))  >= 3)
    prob += ((pulp.lpSum(df.loc[i, 'Pos_WR'] * players_lineup[i] for i in range(num_all)))  <= 4)
    prob += ((pulp.lpSum(df.loc[i, 'Pos_TE'] * players_lineup[i] for i in range(num_all)))  == 1)
    prob += ((pulp.lpSum(df.loc[i, 'Pos_DST'] * players_lineup[i] for i in range(num_all))) == 1)
    prob += ((pulp.lpSum(df.loc[i, 'Pos_All'] * players_lineup[i] for i in range(num_all))) == 9)
    prob += ((pulp.lpSum(df.loc[i, 'MaxUsed'] * players_lineup[i] for i in range(num_all))) == 0)

    #add the salary constraint
    prob += ((pulp.lpSum(df.loc[i, 'Salary'] * players_lineup[i] for i in range(num_all))) <= salary_cap)
    
    #variance constraints - each lineup can't have more than the num overlap of any combination of players in any previous lineups
    for i in range(len(lineups)):
        prob += ((pulp.lpSum(lineups[i][k] * players_lineup[k] for k in range(num_all))) <= overlap)
	
    #solve the problem
    status = prob.solve()
    
    #check if the optimizer found an optimal solution
    if status != pulp.LpStatusOptimal:
        print('Only {} feasible lineups produced'.format(len(lineups)), '\n')
        return None
    
    
    lineup_copy = []
    for i in range(num_all):
    	if players_lineup[i].varValue >= 0.9 and players_lineup[i].varValue <= 1.1:
    		lineup_copy.append(1)
    	else:
    		lineup_copy.append(0)
            
    return lineup_copy


def transform_lineup(lineup, lineup_num):
    positions, names, ids, salaries = [], [], [], []
    game_infos, teams, ppgs, ranks = [], [], [], []

    players_lineup = lineup[:num_all]
    for num, player in enumerate(players_lineup):
        if player == 1:
            positions.append(dk_df.loc[num, 'Position'])
            names.append(dk_df.loc[num, 'Name'])
            ids.append(dk_df.loc[num, 'ID'])
            salaries.append(dk_df.loc[num, 'Salary'])
            game_infos.append(dk_df.loc[num, 'Game Info'])
            teams.append(dk_df.loc[num, 'TeamAbbrev'])
            ppgs.append(dk_df.loc[num, 'AvgPointsPerGame'])
            ranks.append(dk_df.loc[num, 'Rank'])
            
    df = pd.DataFrame({'Position':positions,'Name':names, 'ID':ids, 'Salary':salaries, 
                       'Game Info':game_infos,'TeamAbbrev':teams,'AvgPointsPerGame':ppgs,
                       'Rank':ranks,'LineupNum':lineup_num})
    return df
   
dk_df = pd.read_csv(pred_dir + 'predicted_lineups_wk'+str(curr_wk)+'.csv', index_col = None)
print('Columns Are:',dk_df.columns.tolist())
print('Number Of QBs:', dk_df[(dk_df['Position']=='QB')].shape[0],'\nNumber Of Rbs:',dk_df[(dk_df['Position']=='RB')].shape[0],'\nNumber Of Wrs:',dk_df[(dk_df['Position']=='WR')].shape[0],'\nNumber Of TEs:', dk_df[(dk_df['Position']=='TE')].shape[0])
print("Correct Position Format is: ['QB', 'RB', 'WR', 'TE', 'DST']")
print('DataFrame Format is:', dk_df['Position'].unique().tolist())

filter_df = dk_df.copy()   
filter_df['MaxUsed'] = 0

filter_df['Pos_QB'] = np.where(filter_df['Position']=='QB',1,0)
filter_df['Pos_RB'] = np.where(filter_df['Position']=='RB',1,0)
filter_df['Pos_WR'] = np.where(filter_df['Position']=='WR',1,0)
filter_df['Pos_TE'] = np.where(filter_df['Position']=='TE',1,0)
filter_df['Pos_DST'] = np.where(filter_df['Position']=='DST',1,0)
filter_df['Pos_All'] = 1

num_all = dk_df.shape[0]
num_lineups = 50
num_overlap = 4


l = 1
plyr_at_max, plyr_at_min = [], []
lineups, filled_lineups = [], []
for _ in range(num_lineups):
    lineup = pred_lineup(df = filter_df, lineups = lineups, overlap = num_overlap)
    if lineup:
        lineups.append(lineup)
        filled_lineups.append(transform_lineup(lineup, lineup_num = l))
        all_lineup_df = pd.concat(filled_lineups)
        player_counts = all_lineup_df.groupby(['Name'])['ID'].count().reset_index().rename(columns={'ID':'Plyr_Count'})
        filter_df2 = pd.merge(filter_df, player_counts, how = 'left', on = ['Name'])
        plyr_at_min.extend(filter_df2[filter_df2['Plyr_Count'] >= filter_df2['MinLineups']]['ID'].unique().tolist())
        plyr_at_max.extend(filter_df2[filter_df2['MaxLineups'] <= filter_df2['Plyr_Count']]['ID'].unique().tolist())
        filter_df['MaxUsed'] = np.where(filter_df['ID'].isin(plyr_at_max), 1, 0)
        l += 1
    else:
        break


if len(filled_lineups) > 0:
    all_lineup_df = pd.concat(filled_lineups).drop_duplicates().reset_index(drop=True)
    print('Number Of Total Lineups:',len(all_lineup_df['LineupNum'].unique().tolist()))
    player_counts = all_lineup_df.groupby(['Name'])['ID'].count().reset_index().rename(columns={'ID':'Plyr_Count'})
    player_counts = pd.merge(player_counts, dk_df[['Name','Position']], how = 'left', on = ['Name']).drop_duplicates().reset_index(drop=True)
    
    """ 
        DRAFTKINGS UPLOAD FORMAT 
        QB	RB	RB	WR	WR	WR	TE	FLEX	DST
    """
    transformed_dfs = []
    mini_df = all_lineup_df[['LineupNum','Position','Name','ID']].copy().drop_duplicates().reset_index(drop=True)
    for lnum in mini_df['LineupNum'].unique().tolist():
        temp_df = mini_df[mini_df['LineupNum']==lnum].copy().reset_index(drop=True)
        
        if temp_df[temp_df['Position']=='RB'].shape[0] > 2: #Flex Is A RB
            transform_df = pd.DataFrame({ 'QB':temp_df[temp_df['Position']=='QB']['ID'].values[0],
                                          'RB1':temp_df[temp_df['Position']=='RB']['ID'].values[0],
                                          'RB2':temp_df[temp_df['Position']=='RB']['ID'].values[1],
                                          'WR1':temp_df[temp_df['Position']=='WR']['ID'].values[0],
                                          'WR2':temp_df[temp_df['Position']=='WR']['ID'].values[1],
                                          'WR3':temp_df[temp_df['Position']=='WR']['ID'].values[2],
                                          'TE':temp_df[temp_df['Position']=='TE']['ID'].values[0],
                                          'FLEX':temp_df[temp_df['Position']=='RB']['ID'].values[2],
                                          'DST':temp_df[temp_df['Position']=='DST']['ID'].values[0]}, index = [lnum])
            
        if temp_df[temp_df['Position']=='WR'].shape[0] > 3: #Flex Is A WR
            transform_df = pd.DataFrame({ 'QB':temp_df[temp_df['Position']=='QB']['ID'].values[0],
                                          'RB1':temp_df[temp_df['Position']=='RB']['ID'].values[0],
                                          'RB2':temp_df[temp_df['Position']=='RB']['ID'].values[1],
                                          'WR1':temp_df[temp_df['Position']=='WR']['ID'].values[0],
                                          'WR2':temp_df[temp_df['Position']=='WR']['ID'].values[1],
                                          'WR3':temp_df[temp_df['Position']=='WR']['ID'].values[2],
                                          'TE':temp_df[temp_df['Position']=='TE']['ID'].values[0],
                                          'FLEX':temp_df[temp_df['Position']=='WR']['ID'].values[3],
                                          'DST':temp_df[temp_df['Position']=='DST']['ID'].values[0]}, index = [lnum])
            
        transformed_dfs.append(transform_df)
        
    dk_upload = pd.concat(transformed_dfs)
    dk_upload.columns = ['QB', 'RB', 'RB', 'WR', 'WR', 'WR', 'TE', 'FLEX', 'DST']
    
    
    xlfile = pred_dir + 'Final_Predicted_Wk'+str(curr_wk)+'_Lineups.xlsx'
    with pd.ExcelWriter(xlfile) as writer:  # doctest: +SKIP
        all_lineup_df.to_excel(writer, sheet_name = 'Lineups', float_format='%.2f', index = None)
        player_counts.to_excel(writer, sheet_name = 'PlayerBreakdowns', float_format='%.2f', index = None)
        dk_upload.to_excel(writer, sheet_name = 'DkUploadFormat', float_format='%.2f', index = None)
        
    dk_upload.to_csv(pred_dir + '/Wk'+str(curr_wk)+'_Dk_Lineup_Upload.csv', index = None)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

