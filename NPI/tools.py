import pandas as pd
from pandas import DataFrame

class Verified:
  def __init__(self):
   self.shema = {}
   
  def type_code(self,shema):
    provider_key = {'NPI', 'Ind_PAC_ID'}
    npi_key = {'NPI', 'Entity Type Code'}
    
    self.shema = shema
    if provider_key.issubset(self.shema):
      return 'CMS'
    elif npi_key.issubset(self.shema):
      return 'NPI'
    else:
      return 'unknow'
  