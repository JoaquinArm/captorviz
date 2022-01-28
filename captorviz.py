class data_capture:
  def __init__(self, target, sheet_name=False, date_parse=False,dat=False):
    self.url = target
    self.sheet = sheet_name
    self.parse = date_parse
    self.dat = dat
    self.dataframe = target
    self.df_list = []
  def single_dataset(self):
    url_split = self.url.split("/")
    sheet_id = url_split[5]
    if ' ' in self.sheet:
      sheet = self.sheet.replace(' ', '%20')
      print(sheet)
    else:
      sheet = self.sheet
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet}"
    if self.parse is False:
      return pd.read_csv(url, header=0)
    else:
      return pd.read_csv(url, header=0, parse_dates=self.parse)
  def multiple_datasets(self):
    result = pd.DataFrame()
    for dataset in self.sheet:
      self.sheet = dataset
      temp_query = self.single_dataset()
      temp_query["LOB"] = self.sheet
      result = pd.concat([result,temp_query])
    return result
  def capture(self):
    if type(self.sheet) is list:
      self.dataframe = self.multiple_datasets()
    else:
      self.dataframe = self.single_dataset()
    if self.dat is not False:
      return self.cleaning_columns()
    else:
      return self.dataframe
  def cleaning_columns(self):
    if type(self.dataframe) is list:
      for df in self.dataframe:  
        if type(dat) is list:
          for column in self.dat:
            df = df.drop(column,axis=1)
          self.df_list.append(df)
        else:
            df = df.drop(self.dat,axis=1)
            self.df_list.append(df)
      return self.df_list
    else:
      if type(self.dat) is list:
        for column in self.dat:
          self.dataframe = self.dataframe.drop(column,axis=1)
        return self.dataframe
      else:
        self.dataframe = self.dataframe.drop(self.dat,axis=1)
        return self.dataframe


def data_gathering(url, sheet_name, date_parse=False):
  # Url = Spreadsheet url
  # Sheet_name = Sheet tab name
  url_split = url.split("/")
  sheet_id = url_split[5]
  url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
  if date_parse is False:
    return pd.read_csv(url, header=0)
  else:
    return pd.read_csv(url, header=0, parse_dates=[date_parse])



def dat_insights(dataframe,sheet_name=False,rtn=True,dat=False,date_parse=False):
  if type(dataframe) == str:
    dataframe = data_gathering(dataframe,sheet_name,date_parse)
  if dat is True:
    dataframe = column_cleaner(dataframe,[column for column in dataframe.columns if column not in dat_columns_list])
  null_values = dataframe.isnull().sum().sort_values(ascending=False)
  nulls_sum = (dataframe.isnull().sum()).sum()
  fields_sum = (dataframe.shape[0] * dataframe.shape[1]) - nulls_sum
  nulls_percentile = max(null_values)
  fills_percentile = dataframe.shape[0] - nulls_percentile
  if null_values.shape[0] > 15:
    null_values = null_values[null_values != 0]
  else:
    pass
  fig = make_subplots(rows=2,
                      cols=2,
                      subplot_titles=["<b>CORRECT ENTRIES</b>",
                                      "<b>CORRECT CELLS",
                                      "<b>NULLS/COLUMN</b>"],
                      specs=[[{'type':'domain'},{'type':'domain'}],
                             [{'type':'xy',"colspan":2},{}]])
  fig.add_trace(go.Pie(labels=["Null entries","Valid entries"],
                       values=[nulls_percentile,fills_percentile],
                       hole=0.4,
                       marker_colors=palette,
                       name="Correct entries",
                       title=dataframe.shape[0],legendgroup="apie"),1,1)
  fig.add_trace(go.Pie(labels=["Null cells",
                               "Valid cells"],
                       values=[nulls_sum,
                               fields_sum],
                       hole=0.4,
                       marker_colors=palette,
                       name="Correct cells",
                       title=dataframe.shape[0]*dataframe.shape[1],
                       legendgroup="pie"),1,2)
  fig.add_trace(go.Bar(x=null_values,
                       y=null_values.index,
                       name="Nulls/Column",
                       orientation="h",
                       marker_color="#FC645F",
                       showlegend=False),2,1)
  fig.update_traces(textposition='inside')
  fig.update_layout(height=600,
                    width=800,
                    title=f"<b>{sheet_name.upper()} INSIGHTS</b>",
                    yaxis_title="Columns",
                    xaxis_title="Nulls",
                    font_size=14)
  if rtn == True:
    fig.show(config={'modeBarButtonsToAdd':['drawline',
                                            'drawopenpath',
                                            'drawclosedpath',
                                            'drawcircle',
                                            'drawrect',
                                            'eraseshape']})
  else:
    return fig



def dats_insights(url,df_list,rtn=True,dat=False):
  if rtn == True:
    [dat_insights(url,df,rtn=True,dat=dat) for df in df_list]
  else:
    counter = 0
    return_results = {}
    for df in df_list:
      return_results[df] = dat_insights(url,df,rtn=False,dat=dat)
    return return_results
    


def table_dtype(dataframe,rtn=True):
  # Función que crea reporte de tipo de variables y datos
  # dataframe = objet.dataframe
  # arg = True: use fig.show() otherwise it returns fig for object
  df_dtypes = dataframe.convert_dtypes()
  table_columns = [column for column in df_dtypes.dtypes.index.values]
  table_values = [str(value) for value in df_dtypes.dtypes]
  values = pd.Series(table_values)
  fig = make_subplots(rows=2,
                      cols=2,
                      specs=[[{"type":"table"},{"type":"domain"}],
                             [{"type":"table"},{"type":"domain"}]],
                      subplot_titles=["",
                                      "<b>TYPE",
                                      "",
                                      "<b>CLASS"])
  fig.add_trace(go.Pie(labels=values.value_counts().index,
                       values=values.value_counts(),
                       marker_colors=palette,
                       hole=0.4,
                       title=values.shape[0],
                       legendgroup="pie"),1,2)
  fig.update_traces(textposition='inside')
  fig.add_trace(go.Table(header=dict(values=['Column',
                                             'Data type'],
                                     fill_color="#A8E4A0",
                                     line_color='darkslategray'),
                         cells=dict(values=[table_columns, 
                                            table_values],
                                    line_color='darkslategray')),1,1)
  fig.update_layout(height=500,width=1000,
                    title="<b>DATA TYPE ANALYSIS",
                    font_size=12)
    
  type_var = []
  for value in range(0,len(df_dtypes.dtypes)):
    if "Int" in str(df_dtypes.dtypes[value]):
      type_var.append("Numerical")
    else:
      if len(df_dtypes[df_dtypes.dtypes.index[value]].value_counts().index) <= 2:
        type_var.append("Numerical/Boolean")
      else:
        type_var.append("Categorical")

  pie_values = [values for values in [type_var.count("Categorical"), type_var.count("Numerical"), type_var.count("Numerical/Boolean")] if values !=0]
  pie_labels = str(set(type_var)).replace("{","").replace("}","").replace("'","").split(",")
  fig.add_trace(go.Table(header=dict(values=["Columns",
                                             "Classification"],
                                     fill_color="#A8E4A0",
                                     line_color='darkslategray'),
                         cells=dict(values=[df_dtypes.dtypes.index,type_var])),2,1)
  fig.add_trace(go.Pie(labels=pie_labels,
                       values=pie_values,
                       marker_colors=palette,
                       hole=0.4,
                       legendgroup="pie2"),2,2)
  fig.update_layout(height=800,width=1000,title="<b>DATA TYPE ANALYSIS",font_size=14)
  if rtn == True:
    fig.show(config={'modeBarButtonsToAdd':['drawline',
                                            'drawopenpath',
                                            'drawclosedpath',
                                            'drawcircle',
                                            'drawrect',
                                            'eraseshape']})
  else:
    return fig



def table_ht(dataframe,var_title,plotly=True,rtn=True):
    # Función que crea tablas para head y tail
    # dataframe = objet.dataframe
    # var_title = título para las tablas
    dataframe = dataframe.sort_index(axis=0)
    head = dataframe.head(5)
    tail = dataframe.tail(5)
    if plotly == False:
      dummy_df = pd.DataFrame(np.zeros((1,len(dataframe.columns))),columns=dataframe.columns)
      dummy_df.iloc[0] = ""
      result = pd.concat([head,dummy_df,tail]).reset_index(drop=True)
      return result
    else:
      dummy_df = pd.DataFrame(np.zeros((1,len(dataframe.columns))),columns=dataframe.columns)
      dummy_df.iloc[0] = ""
      result = pd.concat([head,dummy_df,tail]).reset_index(drop=True)
      series = result.transpose().values.tolist()
      fig = make_subplots(rows=1, cols=1, specs=[[{"type":"table"}]])
      fig.add_trace(go.Table(
                      header=dict(values=result.columns,fill_color="#A8E4A0",line_color='darkslategray'),
                      cells=dict(values=series)
                      ),1,1)
      fig.update_layout(height=850,width=4000,title=f"<b>{var_title.upper()} (HEAD / TAIL)",font_size=14)
      if rtn == True:
        fig.show(config={'modeBarButtonsToAdd':['drawline',
                                            'drawopenpath',
                                            'drawclosedpath',
                                            'drawcircle',
                                            'drawrect',
                                            'eraseshape']})
      else:
        return fig



def correlation_matrix(dataframe, name, rtn=True):
  dataframe = dataframe.convert_dtypes()
  fig=make_subplots(cols=1,
                    rows=1)
  z,x = dataframe.corr(method="pearson"),dataframe.corr(method="pearson").columns
  fig.add_trace(go.Heatmap(z = z,
                           x = x,
                           y = x,
                           colorscale="Burg"))
  fig.update_layout(height=800,width=800,title=f"<b>{name} FEATURE CORRELATION MATRIX")
  if rtn == True:
        fig.show(config={'modeBarButtonsToAdd':['drawline',
                                            'drawopenpath',
                                            'drawclosedpath',
                                            'drawcircle',
                                            'drawrect',
                                            'eraseshape']})
  else:
    return fig



def correlation_matrixes(dataframe,name,rtn=True):
  if type(dataframe) is list:
    if type(dataframe[0]) is object:
      for i_var in enumerate(dataframe):
          return correlation_matrix(dataframe[i_var],name,rtn=rtn)
    else:
      if len(dataframe[1]) > 1:
        for dataframe_var in dataframe[1]:
          dataframe = data_gathering(dataframe[0],dataframe_var)
          return correlation_matrix(dataframe,name,rtn=rtn)
      else:
        dataframe = data_gathering(dataframe[0],dataframe[1])
        return correlation_matrix(dataframe,name,rtn=rtn)
  else:
    return correlation_matrix(dataframe,name,rtn=rtn)



def feature_analysis(dataframe,feature,fill=False,rtn=True):
  feature_analysis = dataframe.sort_values(by=feature,axis=0,ascending=True)
  group = feature_analysis.groupby(by=feature,axis=0).mean()
  fig1 = make_subplots(rows=1,cols=1)
  for feature_var in group:
    fig1.add_trace(go.Scatter(x=group[feature_var].index, y=group[feature_var].values, name=feature_var, mode='lines'),1,1)
  fig1.update_layout(title=f"<b> FEATURE MEAN FREQUENCY COMPARISON BY {feature.upper()}",xaxis_title=feature,yaxis_title="Mean Frequency",font_size=14)
  fig2 = make_subplots(rows=round(group.shape[1]/3) + 1,cols=3)
  y = 0
  x = 1
  for feature in group:
      y += 1
      if y == 4 or y == 8 or y == 12:
          y = 1
          x += 1
      if fill == False:
        fig2.append_trace(go.Scatter(x=group[feature].index, y=group[feature].values, name=feature, mode='markers'),x,y)
      else:
        fig2.append_trace(go.Scatter(x=group[feature].index, y=group[feature].values, name=feature, mode='markers',fill=fill),x,y)
        
  fig2.update_layout(width=800,height=800,title="<b> SCATTER PLOT MATRIX VIEW",font_size=14)
  if rtn == True:
    fig1.show(config={'modeBarButtonsToAdd':['drawline',
                                            'drawopenpath',
                                            'drawclosedpath',
                                            'drawcircle',
                                            'drawrect',
                                            'eraseshape']})
    fig2.show(config={'modeBarButtonsToAdd':['drawline',
                                            'drawopenpath',
                                            'drawclosedpath',
                                            'drawcircle',
                                            'drawrect',
                                            'eraseshape']})
  else:
    return fig1,fig2



def column_cleaner(dataframe,columns):
  dummy_df_list = []
  if type(dataframe) is list:
    for df in dataframe:  
      if type(columns) is list:
        for column in columns:
          df = df.drop(column,axis=1)
        dummy_df_list.append(df)
      else:
        df = df.drop(columns,axis=1)
        dummy_df_list.append(df)
    return dummy_df_list
  else:
    if type(columns) is list:
        for column in columns:
            dataframe = dataframe.drop(column,axis=1)
        return dataframe
    else:
        dataframe = dataframe.drop(columns,axis=1)
        return dataframe



def roster_search(url,dataframe,member):
  if type(dataframe) == list:
    stack_df = pd.DataFrame()
    
    for df_temp in dataframe:
      df_temp_var = data_gathering(url,df_temp)
      df_temp_var["LOB"] = df_temp
      stack_df = pd.concat([stack_df,df_temp_var])
  else:
    stack_df = data_gathering(url,dataframe)
  if "@" in member:
    if "Email Address" in stack_df.columns:
      result = stack_df.query(f"`Email Address` == '{member}'")
    else:
      result = stack_df.query(f"`RP email` == '{member}'")
  else:
    result = stack_df.query(f"`Name` == '{member}'")
  return result


def role_search(url,dataframe,role):
  if type(dataframe) == list:
    stack_df = pd.DataFrame()
    for df_temp in dataframe:
      df_temp_var = data_gathering(url,df_temp)
      df_temp_var["LOB"] = df_temp
      stack_df = pd.concat([stack_df,df_temp_var])
  else:
    stack_df = data_gathering(url,dataframe)
  result = pd.concat([stack_df.query(f"`Req ID` == '{role}'"),stack_df.query(f"`Title` == '{role}'")])
  return result