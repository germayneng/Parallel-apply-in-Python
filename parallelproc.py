#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 25 22:38:30 2017

@author: ianlo
"""
import os
import re
import string
import pandas as pd
from multiprocessing import Pool, cpu_count

# set working directory
path = < set your own working directory >
os.chdir(path)

class WithExtraArgs(object):
  def __init__(self, func, **args):
    self.func = func
    self.args = args
  def __call__(self, df):
    return self.func(df, **self.args)


def applyParallel(dfGrouped, func, kwargs):
    with Pool(cpu_count()) as p:
        ret_list = p.map(WithExtraArgs(func, **kwargs), [group for name, group in dfGrouped])
    return pd.concat(ret_list)


def remove_punct(df, dest_col_ind, dest_col_name, src_col_name):
    df.insert(dest_col_ind, dest_col_name, df.apply(lambda x: re.sub('['+string.punctuation+']', '',x[src_col_name]), axis=1, raw=True))
    return df


_number_of_groups = 4

# import training data as dataframe 
df_train = pd.read_csv('largetext.csv')
df_train.insert(0,'grpId',df_train.apply(lambda row: row.name % _number_of_groups, axis=1, raw=True))
df_train.head()

#kwargs = {"dest_col_ind": 4, "dest_col_name": "q1nopunct", "src_col_name": "question1"}
applyParallel(df_train.groupby(df_train.grpId), remove_punct, {"dest_col_ind": 4, "dest_col_name": "q1nopunct", "src_col_name": "question1"})


