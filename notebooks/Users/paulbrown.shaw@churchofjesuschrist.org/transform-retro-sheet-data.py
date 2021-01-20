# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Retro Sheet Data
# MAGIC ### - Reads retro shheet game event files
# MAGIC ### - Transforms event sinto measurable metrics

# COMMAND ----------

# Imports
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, MapType, DoubleType, LongType
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import lit, col, row_number, monotonically_increasing_id, input_file_name, current_timestamp, lead, lag, first, udf, explode, regexp_extract
from pyspark.sql.window import Window
import boto3
import re
import sys
import pandas as pd
import logging
import argparse
import yaml

# COMMAND ----------

# MAGIC %md
# MAGIC run config files notebook to "import" reusable functions related to configuration files

# COMMAND ----------

dbutils.widgets.text("config_file_location", "default")
config_file_widget = dbutils.widgets.get("config_file_location")
 

# COMMAND ----------

# MAGIC %run ./local-lib/config-files

# COMMAND ----------

# MAGIC %run ./local-lib/s3-search

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append Game Data Function

# COMMAND ----------

def append_game_id(arg_target_df, arg_game_df):
    output_cols = arg_target_df.columns
    output_cols.append('game_id')
    game_id_df = arg_game_df.withColumnRenamed('row_id','begin_row_id')['begin_row_id', 'next_row_id', 'game_id' ]
    
    return arg_target_df.join(game_id_df, (arg_target_df['row_id'] > game_id_df['begin_row_id']) & 
                                        (arg_target_df['row_id'] < game_id_df['next_row_id']))[output_cols]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Runner Advance Function

# COMMAND ----------

def parse_runner_advances(arg_runner_advances):

    parsed_advance = []
    if arg_runner_advances == "":
        runner_advances = []
    else:
        runner_advances = arg_runner_advances.split(';')

    for s in runner_advances:
        t = (s.replace(')','').split('('))
        ch_to_num = t[0].replace('B', '0')
        ch_to_num = ch_to_num.replace('H', '4')
        if ch_to_num[1] == 'X' and 'E' in t[1]:
            #print('Safe on error: {}'.format(s))
            ch_to_num = ch_to_num.replace('X', '-')
        parsed_advance.append(tuple([ch_to_num, t[1:]]))
    return parsed_advance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Event Test Function

# COMMAND ----------

def parse_event_text(arg_event_text):
    
    event_parts = arg_event_text.split('.')
    
    play_text = event_parts.pop(0).split('/')
    basic_play_text = play_text.pop(0)
    
    play_modifiers = play_text
    
    if len(event_parts) > 0:
        runner_advances = event_parts[0]
        """runner_advances = event_parts[0].split(';')
        parsed_advance = []
        for s in runner_advances:
            t = (s.replace(')','').split('('))
            ch_to_num = t[0].replace('B', '0')
            ch_to_num = ch_to_num.replace('H', '4')
            if ch_to_num[1] == 'X' and 'E' in t[1]:
                print('Safe on error: {}'.format(s))
                ch_to_num = ch_to_num.replace('X', '-')
            parsed_advance.append(tuple([ch_to_num, t[1:]]))
        runner_advances = parsed_advance"""
    else:
        runner_advances = ""
    
    return basic_play_text, play_modifiers, runner_advances

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Plate Appearance Dict Function

# COMMAND ----------

def build_plate_appearance_dict(play_description):
    batter_dict = {
    'plate_appearance': 0,
    'at_bat': 0,
    'single': 0,
    'double': 0,
    'triple': 0,
    'home_run': 0,
    'walk': 0,
    'intentional_walk': 0,
    'sac_fly': 0,
    'sac_hit': 0,
    'hit_by_pitch': 0,
    'fielders_choice': 0,
    'strike_out': 0,
    'runs_batted_in': 0,
    'catcher_interference': 0,
    'reached_on_error': 0}
    
    # walk, intentional walk, sacrifice, catcher's interference are not at bats
    not_at_bat_events = ['IW', 'W', 'C', 'HP']
    outs = ['1', '2', '3', '4', '5', '6', '7', '8', '9', 'K']
    
    res_play_text, res_play_modifiers, res_runner_advances = parse_event_text(play_description)
    
    batter_dict['play_text']      = res_play_text
    batter_dict['play_modifiers'] = res_play_modifiers
    batter_dict['runner_advances'] = res_runner_advances
    base_running_events = 'BK', 'CS', 'DI', 'OA', 'PB', 'WP', 'PO', 'POCS', 'SB' 
    no_play_events = 'NP', 'FLE'  
    at_bat_not_complete = base_running_events + no_play_events
    
    if not res_play_text.startswith(tuple(at_bat_not_complete)):
        batter_dict['plate_appearance'] = 1
        rbi =0
        runner_advances = parse_runner_advances(res_runner_advances)
        for adv in runner_advances:
            if '-4' in adv[0]:
                if sum(('NR' == mod or 'NORBI' == mod) for mod in adv[1]) == 0:
                    rbi += 1
        
        if res_play_text.startswith(tuple(outs)) and not('E' in res_play_text):
            if 'SF' in res_play_modifiers:
                batter_dict['sac_fly'] = 1
            elif 'SH' in res_play_modifiers:
                batter_dict['sac_hit'] = 1
            else:
                batter_dict['at_bat'] = 1
                if res_play_text.startswith('K'):
                    batter_dict['strike_out'] = 1
                elif 'FO' in res_play_modifiers:
                    batter_dict['fielders_choice'] = 1
        else:
            if res_play_text.startswith(tuple(not_at_bat_events)):
                if res_play_text.startswith('I'):
                    batter_dict['intentional_walk'] = 1
                    batter_dict['walk'] = 1
                elif res_play_text.startswith('W'):
                    batter_dict['walk'] = 1
                elif res_play_text.startswith('HP'):
                    batter_dict['hit_by_pitch'] = 1
                elif res_play_text.startswith('C'):
                    batter_dict['catcher_interference'] = 1
                else:
                    print('?????? - {}'.format(res_play_text))
            else:
                batter_dict['at_bat'] = 1
                if res_play_text.startswith('S'):
                    batter_dict['single'] = 1
                elif res_play_text.startswith('D'):
                    batter_dict['double'] = 1
                elif res_play_text.startswith('T'):
                    batter_dict['triple'] = 1
                elif res_play_text.startswith('H'):
                    batter_dict['home_run'] = 1
                    rbi += 1
                elif 'E' in res_play_text:
                    batter_dict['reached_on_error'] = 1
                elif res_play_text.startswith('FC'):
                    batter_dict['fielders_choice'] = 1
                else:
                    print('?????? - {}'.format(res_play_text))
                    
        batter_dict['runs_batted_in'] = rbi
    return batter_dict

udf_build_plate_appearance_dict = udf(build_plate_appearance_dict, MapType(StringType(), StringType()))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Init Runner Event Dict Function

# COMMAND ----------

def init_runner_event_dict(arg_play_dict):
    return  arg_play_dict.copy()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Init Base Runner Play Function

# COMMAND ----------

def init_base_runner_play(arg_row):
    #print('advance_runners with {}'.format(arg_row))
    
    return {
    'row_id': arg_row['row_id'],
    'record_type': 'play-runner',
    'inning': arg_row['inning'],
    'game_id': arg_row['game_id'],
    'season': arg_row['season'],
    'home_team': arg_row['home_team'],
    'player_id': None,
    'count_on_batter': arg_row['count_on_batter'],
    'pitch_sequence': arg_row['pitch_sequence'],
    'event_text': arg_row['event_text'],
    'date_ingested': arg_row['date_ingested'],
    'input_file_name': arg_row['input_file_name'],
    'play_text': arg_row['play_text'],
    'play_modifiers': arg_row['play_modifiers'],
    'runner_advances': arg_row['runner_advances'],
    'runner_dict': None,
    'run': 0,
    'stolen_base': 0,
    'caught_stealing': 0,
    'picked_off': 0,
    'defensive_indifference': 0,
    'advanced_on_wild_pitch': 0,
    'advanced_on_passed_ball': 0,
    'advanced_on_other': 0} 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Pinch Runner Function

# COMMAND ----------

def apply_pinch_runner(arg_pinch_runner, arg_replaced_runner, arg_runner_dict):

    for runner in arg_runner_dict.items():
        if runner[1] == arg_replaced_runner:
            arg_runner_dict[runner[0]] = arg_pinch_runner
            break

    return 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advance Runners Function

# COMMAND ----------

def advance_runners(arg_runner_dict, arg_row):
    
    # process pinch runners
    if arg_row['play_text'] == 'NP' and arg_row['fielding_position'] == '12':
        apply_pinch_runner(arg_row['subbing_player_id'], arg_row['subbed_player_id'], arg_runner_dict)
    
    runner_plays = []
    advances = parse_runner_advances(arg_row['runner_advances'])    
    runner_list = list(arg_runner_dict.values())
    runner_list[0] = arg_row['player_id']
    init_runner_list = runner_list.copy()
    #print('{} {} {}'.format(arg_runner_dict,arg_row['play_text'],advances))

    #process stolen bases    
    sb_list = re.findall('SB(\d)', arg_row['play_text'])
    sb_advances = []
    start_pos = 0
    for sb in sb_list:
        if sb == 'H':
            sb_advances.append(('3-4', []))
            start_pos = 3
        else:
            start_pos = int(sb) - 1
            sb_advances.append((str(start_pos)+'-'+sb, []))
            
        
        base_runner_play = init_base_runner_play(arg_row)
        base_runner_play['stolen_base'] = 1
        base_runner_play['player_id'] = runner_list[start_pos]
        base_runner_play['runner_dict'] = init_runner_list
        runner_plays.append(base_runner_play)
    
    #process caught stealing    
    cs_list = re.findall('CS(\d)', arg_row['play_text'])
    cs_advances = []
    start_pos = 0
    for cs in cs_list:
        if cs == 'H':
            start_pos = 3
            if 'E' in arg_row['play_text']:
                cs_advances.append(('3-4', []))
            else:
                cs_advances.append(('3X4', []))
        else:
            start_pos = int(cs) - 1
            if 'E' in arg_row['play_text']:
                cs_advances.append((str(start_pos)+'-'+cs, []))
            else:
                cs_advances.append((str(start_pos)+'X'+cs, []))
            
        
        base_runner_play = init_base_runner_play(arg_row)
        base_runner_play['caught_stealing'] = 1
        base_runner_play['player_id'] = runner_list[start_pos]
        base_runner_play['runner_dict'] = init_runner_list
        runner_plays.append(base_runner_play)

    #print(sb_advances)
    # modify explicit batter advance
    #print(advances)
    explicit_batter_adv = False
    for adv in advances:
        if adv[0][0]=='0':
            explicit_batter_adv = True
   
    # merge advances and stolen base advances
    advances = sorted(advances+sb_advances+cs_advances, reverse=True)
    
    prev_start_pos = -1
    
    for adv in advances:
        start_pos = int(adv[0][0])
        if start_pos != prev_start_pos:
            prev_start_pos = start_pos
            end_pos = int(adv[0][2])
            call = adv[0][1]

            if call == '-' and end_pos > start_pos:
                if end_pos == 4:
                    #print('{} - scores'.format(runner_list[start_pos]))
                    base_runner_play = init_base_runner_play(arg_row)
                    base_runner_play['run'] = 1
                    base_runner_play['player_id'] = runner_list[start_pos]
                    base_runner_play['runner_dict'] = init_runner_list
                    runner_plays.append(base_runner_play)
                else:
                    runner_list[end_pos] = runner_list[start_pos]
                runner_list[start_pos] = None
            elif call == 'X':
                runner_list[start_pos] = None
            elif end_pos > start_pos:
                print('unrecognized call: {}'.format(call))

    end_pos = 0
    # advance the batter
    if not(explicit_batter_adv):
        end_pos = implicit_batter_end_pos(arg_row)
        if end_pos < 4:
            runner_list[end_pos] = runner_list[0]
        
    if end_pos == 4:
        #print('{} - scores'.format(arg_row['player_id']))
        base_runner_play = init_base_runner_play(arg_row)
        base_runner_play['runner_dict'] = init_runner_list
        base_runner_play['run'] = 1
        base_runner_play['player_id'] = arg_row['player_id']
        runner_plays.append(base_runner_play)
    
    arg_runner_dict['batter'] = None        
    arg_runner_dict['first'] = runner_list[1]
    arg_runner_dict['second'] = runner_list[2]
    arg_runner_dict['third'] = runner_list[3]
        
    return arg_runner_dict, runner_plays

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Runner DF Function

# COMMAND ----------

def build_runner_df(arg_play_df):
    prev_game_id = ''
    prev_inning  = ''
    prev_home_team = ''
    init_runner_dict = {'batter': None, 'first': None, 'second': None, 'third': None}
    runner_play_list = []

    for index, row in arg_play_df.iterrows():
        if row['game_id'] != prev_game_id:
            runner_dict = init_runner_event_dict(init_runner_dict)
            prev_game_id = row['game_id']
            prev_home_team = row['home_team']

        if row['inning'] != prev_inning or row['home_team'] != prev_home_team:
            runner_dict =init_runner_event_dict(init_runner_dict)
            prev_inning = row['inning']
            prev_home_team = row['home_team']
            
        runner_dict, runner_plays = advance_runners(runner_dict, row)
        runner_play_list += runner_plays        

    return pd.DataFrame(runner_play_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Implicit Batter End Pos Function

# COMMAND ----------

def implicit_batter_end_pos(arg_row):
    
    rtn_pos = 0
    if arg_row['single']+arg_row['walk']+arg_row['hit_by_pitch']+arg_row['fielders_choice']+\
       arg_row['catcher_interference']+arg_row['reached_on_error']+arg_row['intentional_walk'] > 0:
        rtn_pos = 1
    elif arg_row['double']:    
        rtn_pos = 2
    elif arg_row['triple']:    
        rtn_pos = 3
    elif arg_row['home_run']:    
        rtn_pos = 4
        
    return rtn_pos

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Get Retro Stat Dataframe Function

# COMMAND ----------

def get_retro_stat_dataframe(arg_bucket, arg_config, arg_spark):

    search_year = arg_config['search_year']
    logging.info('search_year: {}'.format(search_year))
    search_pattern = '.*{}\D\D\D\.EV[AN]'.format(search_year)
    logging.info('search_pattern: {}'.format(search_pattern))
    prefix_filter = arg_config['prefix_filter']
    logging.info('prefix_filter: {}'.format(prefix_filter))
    
    files_to_read = get_files_to_read(arg_bucket, prefix_filter, search_pattern)   

    fields = [StructField('record_type', StringType(), nullable=False),
            StructField('f1', StringType(), nullable=True),
            StructField('f2', StringType(), nullable=True),
            StructField('f3', StringType(), nullable=True),
            StructField('f4', StringType(), nullable=True),
            StructField('f5', StringType(), nullable=True),
            StructField('f6', StringType(), nullable=True)]
    input_schema = StructType(fields)

    df = (arg_spark                              # Our SparkSession & Entry Point
    .read                                          # Our DataFrameReader
    .csv(files_to_read, schema=input_schema) # Returns an instance of DataFrame 
            .withColumn("date_ingested", current_timestamp())
            .withColumn("input_file_name", input_file_name())
            .withColumn("row_id", monotonically_increasing_id())
        .cache()                                   # Cache the DataFrame
    )
    logging.info('Input Data Frame Shape {} x {}'.format(df.count(), len(df.dtypes)))

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Game Dataframe Function

# COMMAND ----------

def create_game_dataframe(arg_df):

    windowSpec  = Window.partitionBy("record_type").orderBy("row_id")
    season_pattern = '\D{3}([\d]{4})'
    game_cols = ['row_id', 'record_type', 'f1', 'date_ingested', 'input_file_name']
    
    out_df = arg_df[arg_df['record_type']=='id'][game_cols].withColumnRenamed("f1","game_id")
    out_df = out_df.withColumn('season', regexp_extract(col("game_id"), season_pattern, 1))\
                   .withColumn("next_row_id",lead("row_id",1, sys.maxsize).over(windowSpec))
    out_df.cache()

    logging.info('Game Data Frame Shape {} x {}'.format(out_df.count(), len(out_df.dtypes)))

    return out_df

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create Player Sub Dataframe Function

# COMMAND ----------

def create_player_sub_dataframe(arg_df, arg_game_df):

    player_game_columns = ['row_id', 'record_type', 'f1', 'f2', 'f3', 'f4', 'f5', 'date_ingested', 'input_file_name']
    player_game_cols_renamed = ['row_id', 'play_sub_type', 'player_id', 'player_name', 'home_team', 'batting_order', 
                                'fielding_position', 'date_ingested', 'input_file_name']

    player_game_df = arg_df[arg_df['record_type'].isin(['start', 'sub'])][player_game_columns].toDF(*player_game_cols_renamed)
    player_game_df = append_game_id(player_game_df, arg_game_df)

    sort_columns = ['game_id', 'home_team', 'batting_order', 'row_id']
    windowSpec  = Window.partitionBy('game_id', 'home_team', 'batting_order').orderBy('row_id')
    cols_to_keep = ['row_id', 'subbed_player_id', 'player_id', 'batting_order', 'fielding_position']
    sub_cols_renamed = ['sub_row_id', 'subbed_player_id', 'subbing_player_id', 'batting_order', 'fielding_position']

    out_df = player_game_df.sort(*sort_columns).withColumn('subbed_player_id',lag('player_id',1, None).over(windowSpec))
    out_df = out_df.filter(out_df['play_sub_type']=='sub')[cols_to_keep].toDF(*sub_cols_renamed)
    out_df.cache()
    logging.info('Player Sub Data Frame Shape {} x {}'.format(out_df.count(), len(out_df.dtypes)))

    return out_df

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create Game Dim Function

# COMMAND ----------

def create_game_dim(arg_df, arg_game_df):

    game_info_columns = ['row_id', 'f1', 'f2', 'date_ingested', 'input_file_name']
    game_info_columns_rename = ['row_id', 'info_key', 'info_value', 'date_ingested', 'input_file_name']
    game_info_df = arg_df[arg_df['record_type']=='info'][game_info_columns].toDF(*game_info_columns_rename)
    game_info_df = append_game_id(game_info_df, arg_game_df)

    distinct_keys = ([x.info_key for x in game_info_df.select('info_key').distinct().collect()])
    game_info_df = game_info_df.groupby('game_id').pivot('info_key', distinct_keys).agg(first('info_value'))

    # add season to df
    game_info_df = arg_game_df['game_id', 'season'].join(game_info_df, on='game_id')
    game_info_df.cache()
    logging.info('Game Dim Data Frame Shape {} x {}'.format(game_info_df.count(), len(game_info_df.dtypes)))

    return game_info_df

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create Batter Metrics Dataframe Function

# COMMAND ----------

def create_batter_metrics_dataframe(arg_df, arg_game_df, arg_sub_df):

    play_columns = ['row_id', 'f1', 'f2', 'f3', 'f4', 'f5', 'f6', 'date_ingested', 'input_file_name']
    play_columns_rename = ['row_id', 'inning', 'home_team', 'player_id', 'count_on_batter', 'pitch_sequence', 
                        'event_text', 'date_ingested', 'input_file_name']
    play_df = arg_df[arg_df['record_type']=='play'][play_columns].toDF(*play_columns_rename)
    play_df = append_game_id(play_df, arg_game_df)

    tdf = play_df.withColumn('play_dict', udf_build_plate_appearance_dict(play_df.event_text))[['row_id', 'event_text', 'play_dict']]
    tdf = tdf.select(['row_id', explode(col('play_dict'))])

    distinct_keys = ([x.key for x in tdf.select('key').distinct().collect()])
    tdf = tdf.groupby('row_id').pivot('key', distinct_keys).agg(first('value'))

    long_cols = ['runs_batted_in', 'single', 'reached_on_error', 'intentional_walk', 'triple', 'sac_hit', 'sac_fly', 
                'walk', 'home_run', 'fielders_choice', 'catcher_interference', 'plate_appearance', 'strike_out', 
                'hit_by_pitch', 'double', 'at_bat']
    for col_to_cast in long_cols:
        tdf = tdf.withColumn(col_to_cast, tdf[col_to_cast].cast(LongType()))

    play_df = play_df.join(tdf, on='row_id')

    play_df = play_df.join(arg_sub_df, play_df['row_id'] == (arg_sub_df['sub_row_id']-1), how='left')

    play_df = arg_game_df['game_id', 'season'].join(play_df, on='game_id')
    play_df.cache()
    logging.info('Play Data Frame Shape {} x {}'.format(play_df.count(), len(play_df.dtypes)))

    return play_df

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create Play Dim Function

# COMMAND ----------

def create_play_dim(arg_batter_metrics_df):

    play_dim_cols = ['inning', 'home_team', 'pitch_sequence', 'count_on_batter']
    play_dim_cols_rename = ['inning', 'bottom_inning', 'pitch_sequence', 'count_on_batter']
    out_df = arg_batter_metrics_df[play_dim_cols].toDF(*play_dim_cols_rename).dropDuplicates()

    out_df = out_df.withColumn('balls', out_df.count_on_batter.substr(1, 1))
    out_df = out_df.withColumn('strikes', out_df.count_on_batter.substr(2, 1))

    logging.info('Play Dim Data Frame Shape {} x {}'.format(out_df.count(), len(out_df.dtypes)))

    return out_df

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create Runner Metrics Dataframe Function

# COMMAND ----------

def create_runner_metrics_dataframe(arg_batter_metrics_df, arg_spark):

    # x returned as Row object
    season_list = ([x.season for x in arg_batter_metrics_df.select('season').distinct().collect()])

    df_list = []
    for season in season_list:
        logging.info('ready to crete pandas df for {}'.format(season))
        pdf = arg_batter_metrics_df[arg_batter_metrics_df['season'] == season].toPandas()
        logging.info('pandas df complete')
        runner_df = build_runner_df(pdf)
        logging.info('runner df complete')
        df_list.append(arg_spark.createDataFrame(runner_df))
        logging.info('pandas to spark complete')

    raw_runner_fact_df = df_list.pop()
    for df in df_list:
        raw_runner_fact_df = raw_runner_fact_df.union(df)
        logging.info('union complete ')
    raw_runner_fact_df.cache()
    logging.info('Runner Data Frame Shape {} x {}'.format(raw_runner_fact_df.count(), 
                                                          len(raw_runner_fact_df.dtypes)))

    return raw_runner_fact_df

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write Partitioned Offensive Metrics Function

# COMMAND ----------

def write_partitioned_offensive_metrics(arg_batter_metrics_df, arg_runner_metrics_df, arg_output_dir):
    
    metric_df_output_columns = ['game_id', 'season', 'row_id', 'inning', 'home_team', 'player_id', 'count_on_batter', 
                                'pitch_sequence', 'event_text', 'play_text', 'play_modifiers', 'runner_advances', 
                                'at_bat', 'run', 'single', 'double', 'triple', 'home_run', 'runs_batted_in',  
                                'walk', 'strike_out', 'stolen_base', 'caught_stealing', 'plate_appearance', 
                                'hit_by_pitch', 'sac_hit', 'sac_fly', 'intentional_walk','fielders_choice', 
                                'catcher_interference', 'reached_on_error', 'picked_off', 'defensive_indifference', 
                                'advanced_on_wild_pitch', 'advanced_on_passed_ball', 'advanced_on_other',
                                'date_ingested', 'input_file_name']

    raw_play_fact_df = arg_batter_metrics_df[arg_batter_metrics_df['play_text'] != 'NP']\
                    .withColumn('run',lit(0).cast(DoubleType()))\
                    .withColumn('stolen_base', lit(0))\
                    .withColumn('caught_stealing', lit(0))\
                    .withColumn('run', lit(0))\
                    .withColumn('stolen_base', lit(0))\
                    .withColumn('caught_stealing',lit(0))\
                    .withColumn('picked_off', lit(0))\
                    .withColumn('defensive_indifference', lit(0))\
                    .withColumn('advanced_on_wild_pitch', lit(0))\
                    .withColumn('advanced_on_passed_ball', lit(0))\
                    .withColumn('advanced_on_other', lit(0))[metric_df_output_columns].\
                union(arg_runner_metrics_df\
                    .withColumn('runs_batted_in',lit(0))\
                    .withColumn('single', lit(0))\
                    .withColumn('reached_on_error', lit(0))\
                    .withColumn('intentional_walk', lit(0))\
                    .withColumn('triple', lit(0))\
                    .withColumn('sac_hit',lit(0))\
                    .withColumn('sac_fly', lit(0))\
                    .withColumn('walk', lit(0))\
                    .withColumn('home_run', lit(0))\
                    .withColumn('fielders_choice', lit(0))\
                    .withColumn('catcher_interference', lit(0))\
                    .withColumn('plate_appearance', lit(0))\
                    .withColumn('strike_out', lit(0))\
                    .withColumn('hit_by_pitch', lit(0))\
                    .withColumn('double', lit(0))\
                    .withColumn('at_bat', lit(0))[metric_df_output_columns])

    raw_play_fact_df.cache()
    parquet_table = 'raw_play_fact'
    seasons = ([x.season for x in raw_play_fact_df.select('season').distinct().collect()])
    non_partition_cols = raw_play_fact_df.columns
    non_partition_cols.remove('season')

    logging.info('*    writing raw play fact: {} x {} ({} partitions)'.format(raw_play_fact_df.count(), 
                                                                          len(raw_play_fact_df.columns),
                                                                          len(seasons))) 
    logging.debug('*     with schema:')
    for item in (raw_play_fact_df.schema.simpleString().split(',')):
        logging.debug('*        {}'.format(item))

    """for season in seasons:
        raw_play_fact_df[raw_play_fact_df['season']==season][non_partition_cols].write.mode('overwrite').\
              parquet(arg_output_dir+'/'+parquet_table+'/season={}'.format(season))"""

    #raw_play_fact_df.coalesce(4).write.mode('overwrite').\
    raw_play_fact_df.write.mode('overwrite').\
              parquet(arg_output_dir+'/'+parquet_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write Player Substitution Function

# COMMAND ----------

def write_player_substitution_data(arg_batter_metrics_df, arg_output_dir):
    
    sub_df_columns = ['game_id', 'season', 'row_id', 'inning', 'home_team', 'player_id', 'count_on_batter', 
                    'pitch_sequence', 'event_text','sub_row_id', 'subbed_player_id', 'subbing_player_id', 
                    'batting_order', 'fielding_position', 'date_ingested', 'input_file_name']
    play_sub_df = arg_batter_metrics_df[arg_batter_metrics_df['play_text'] == 'NP'][sub_df_columns]                            

    parquet_table = 'player_substitutions'

    logging.info('*    writing player substition data: {} x {}'.format(play_sub_df.count(), 
                                                                       len(play_sub_df.columns))) 
    logging.debug('*     with schema:')
    for item in (play_sub_df.schema.simpleString().split(',')):
        logging.debug('*        {}'.format(item))

    play_sub_df.write.mode('overwrite').parquet(arg_output_dir+'/'+ parquet_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write Parquet Function

# COMMAND ----------

def write_parquet(arg_df, arg_parquet_location):

    arg_df.write.mode('overwrite').parquet(arg_parquet_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ## main Function

# COMMAND ----------

def main(arg_config):
    # Establish Spark configuration
    start_time = datetime.now()
    logging.info('Transform Retro Stat Data started @ {}'.format(start_time))
    #spark = create_spark_session()

    #spark.conf.set("spark.sql.shuffle.partitions", "3")
    # https://docs.databricks.com/spark/latest/spark-sql/spark-pandas.html
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    # Identify root directory for input and output
    input_bucket = arg_config['bucketName']
    outputDir = arg_config['outputDir']
    outputDir = outputDir.format(input_bucket)
    write_play_dim = arg_config.get('write_play_dim')
    logging.info('input_bucket: {}'.format(input_bucket))
    logging.info('outputDir: {}'.format(outputDir))

    ###########################################################################
    #  Read Input files
    ###########################################################################

    df = get_retro_stat_dataframe(input_bucket, arg_config, spark)


    ###########################################################################
    #  Perform Transformations
    ###########################################################################
        ###########################################################################
        #  Create Game Dataframe
        ###########################################################################
   
    game_df = create_game_dataframe(df)

        ###########################################################################
        #  Create Player Substitution Dataframe
        ###########################################################################
    
    sub_df = create_player_sub_dataframe(df, game_df)

        ###########################################################################
        #  Create Raw Game Dimension
        ###########################################################################
    
    raw_game_dim_df = create_game_dim(df, game_df)

        ###########################################################################
        #  Create Batter Metrics Dataframe
        ###########################################################################

    batter_metrics_df = create_batter_metrics_dataframe(df, game_df, sub_df)
 
        ###########################################################################
        #  Create Raw Play Dimension Dataframe
        ###########################################################################


    if write_play_dim:
        raw_play_dim_df = create_play_dim(batter_metrics_df)
    else:
        logging.info('skipping play_dim')

        ###########################################################################
        #  Create Runner Metrics Dataframe
        ###########################################################################

    runner_metrics_df = create_runner_metrics_dataframe(batter_metrics_df, spark)

    ###########################################################################
    #  Write to Cloud Storage
    ###########################################################################
        ###########################################################################
        #  Write Partitioned Offensive Metrics
        ###########################################################################

    write_partitioned_offensive_metrics(batter_metrics_df, runner_metrics_df, outputDir)

        ###########################################################################
        #  Write Player Substitution Data
        ###########################################################################

    write_player_substitution_data(batter_metrics_df, outputDir)

        ###########################################################################
        #  Write Raw Play / Game Dimension Data
        ###########################################################################
    
        
    if write_play_dim:
        logging.info('*    writing play dimension: {} x {}'.format(raw_play_dim_df.count(), 
                                                               len(raw_play_dim_df.columns))) 
        logging.debug('*     with schema:')
        for item in (raw_play_dim_df.schema.simpleString().split(',')):
            logging.debug('*        {}'.format(item))
        write_parquet(raw_play_dim_df, outputDir+'/raw_play_dim')

    logging.info('*    writing game dimension: {} x {}'.format(raw_game_dim_df.count(), 
                                                               len(raw_game_dim_df.columns))) 
    logging.debug('*     with schema:')
    for item in (raw_game_dim_df.schema.simpleString().split(',')):
        logging.debug('*        {}'.format(item))
    write_parquet(raw_game_dim_df, outputDir+'/raw_game_dim')


    ###########################################################################
    #  Wrap Up
    ###########################################################################

    logging.info('*'*80)
    logging.info('*')
    logging.info('* Processing Complete @ {}'.format(datetime.now()))
    logging.info('*')
    logging.info('*')
    logging.info('*  Total Execution Time: {}'.format(datetime.now() - start_time))
    logging.info('*')
    logging.info('*'*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Script Entry Point

# COMMAND ----------


configfile = get_config_file(config_file_widget)
#spark = create_spark_session()
#log4jLogger = spark._jvm.org.apache.log4j
#logger = log4jLogger.LogManager.getLogger(__name__)
#FORMAT = '%(asctime)-15s %(message)s'
FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#logging.basicConfig(format=FORMAT, level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)
root = logging.getLogger()
#root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter(FORMAT)
handler.setFormatter(formatter)
root.handlers = []
root.addHandler(handler)
main(configfile)