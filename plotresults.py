import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#formatting
sns.set_style('dark')
colors = ["#0054e6", "#d81159", "#000000"]
sns.set_palette(sns.color_palette(colors))


def CreatePlot(df):
    test_name = df["TestName"].iloc[0]
    encryption_algo = df["EncryptionAlgo"].iloc[0]
    
    #eyeball estimates
    order = 1
    if encryption_algo == "Randomized" and test_name != "RunCreates" or test_name == "FindWhereDOB":
        order = 2
    
    fitReg = True
    if test_name == "RunCreates":
        fitReg = False
    
    titleMap = {"RunCreates": "Create Test",
                "RunFinds" : "Find by Name Test",
                "RunUpdates" : "Update Phone by Name Test",
                "RunDeletes" : "Delete by Name Test",
                "FindWhereDOB" : "Find Where DOB Test"}
    
    
    scatter_plot = sns.lmplot(
    data=df,
    x='DatabaseSize',
    y='Time',
    hue='SchemaName',
    ci=None,  
    scatter=True,
    order=order,
    height=5.5,
    aspect=1.5,
    line_kws={'linewidth': .5},
    legend = False,
    fit_reg=fitReg
    )


    plt.legend(loc='upper left', bbox_to_anchor=(0.0, 1.0), title='Schema Name')
    plt.xlabel('Database Size (Records)')
    plt.ylabel('Time (Seconds)')
    plt.title(f'{titleMap[test_name]} for {encryption_algo} Encryption')
    plt.tight_layout()

    plt.show()
    
def CreateBarGraph(df):

    average_run_creates = df.groupby('SchemaName')['Time'].mean().reset_index()

    plt.figure(figsize=(8, 6))
    bar_chart = sns.barplot(
        data=average_run_creates,
        x='SchemaName',
        y='Time',
    )

    plt.xlabel('Schema Name')
    plt.ylabel('Average RunCreates')
    plt.title('Average RunCreates')

    plt.show()


df = pd.read_csv("results/fullResults.csv")

encryptionTypes = df["EncryptionAlgo"].unique()
testTypes = df["TestName"].unique()

for e in encryptionTypes:
    for t in testTypes:
        dfTargeted = df.loc[((df["EncryptionAlgo"] == e) & (df["TestName"] == t))]
        if e == "Randomized":
            dfTargeted = dfTargeted.loc[((df["DatabaseSize"] < 25000))] #time gets too large after this
        CreatePlot(dfTargeted)
        
        if t == "RunCreates":
            CreateBarGraph(dfTargeted)
        