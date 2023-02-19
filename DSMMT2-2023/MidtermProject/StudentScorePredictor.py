import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn import linear_model
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error



def gradePrediction(inputlist):
    studentData=pd.read_csv("E:\\AdvancedPython\\MidTerm_Project\\Data\\student_data.csv",header=0)
        
    # Here we will convert all the binary columns and categorical columns to integers.
    # 0 stands for U and 1 stands for R. [U=Urban, R=Rural]
    studentData['b_address'] = studentData['address'].apply(lambda x: 0 if x == 'U' else 1)
    studentData['b_address'].value_counts()

    # LE3 = Less than 3. [0], GE3 = Greater than 3.[1]
    studentData['b_famsize'] = studentData['famsize'].apply(lambda x: 0 if x == 'LE3' else 1)
    studentData['b_famsize'].value_counts()

    # T = Parents are living together [0], A = Parents living apart. [1]
    studentData['b_Pstatus'] = studentData['Pstatus'].apply(lambda x: 0 if x == 'T' else 1)
    studentData['b_Pstatus'].value_counts()

    # 0 = no and 1 = yes
    studentData['b_famsup'] = studentData['famsup'].apply(lambda x: 0 if x == 'no' else 1)
    studentData['b_famsup'].value_counts()

    # 0 = no and 1 = yes
    studentData['b_paidxtraclasses'] = studentData['paid'].apply(lambda x: 0 if x == 'no' else 1)
    studentData['b_paidxtraclasses'].value_counts()

    # 0 = no and 1 = yes
    studentData['b_xtraactivities'] = studentData['activities'].apply(lambda x: 0 if x == 'no' else 1)
    studentData['b_xtraactivities'].value_counts()

    # 0 = no and 1 = yes
    studentData['b_higher_education'] = studentData['higher'].apply(lambda x: 0 if x == 'no' else 1)
    studentData['b_higher_education'].value_counts()

    # 0 = no and 1 = yes
    studentData['b_internet'] = studentData['internet'].apply(lambda x: 0 if x == 'no' else 1)
    studentData['b_internet'].value_counts()

    # 0 = no and 1 = yes
    studentData['b_romantic'] = studentData['romantic'].apply(lambda x: 0 if x == 'no' else 1)
    studentData['b_romantic'].value_counts()

    # 0 = no and 1 = yes
    studentData['b_nursery'] = studentData['nursery'].apply(lambda x: 0 if x == 'no' else 1)
    studentData['b_nursery'].value_counts()

    # 0 = mother, 1 = father, other = 2
    studentData['b_guardian'] = studentData['guardian'].apply(lambda x: 0 if x == 'mother' else (1 if x=='father' else 2))
    studentData['b_guardian'].value_counts()

    #0= home, 1= reputation, 2 = course, 3 = other
    studentData['b_reason'] = studentData['reason'].apply(lambda x: 0 if x == 'home' else (1 if x=='reputation' else (2 if x=='course' else 3)))
    studentData['b_reason'].value_counts()

    # 0 = GP, 1 = MS
    studentData['b_school'] = studentData['school'].apply(lambda x: 0 if x == 'GP' else 1)
    studentData['b_school'].value_counts()

    # 0 = no and 1 = yes
    studentData['b_schoolsup'] = studentData['schoolsup'].apply(lambda x: 0 if x == 'no' else 1)
    studentData['b_schoolsup'].value_counts()

    # 0 = F, 1 = M
    studentData['b_sex'] = studentData['sex'].apply(lambda x: 0 if x == 'F' else 1)
    studentData['b_sex'].value_counts()

    #0= at_home, 1= health, 2 = services, 3 = teacher , 4 = other
    studentData['b_mjob'] = studentData['Mjob'].apply(lambda x: 0 if x == 'at_home' else (1 if x=='health' else (2 if x=='services' else (3 if x=='teacher' else 4))))
    studentData['b_mjob'].value_counts()

    #0= at_home, 1= health, 2 = services, 3 = teacher , 4 = other
    studentData['b_fjob'] = studentData['Fjob'].apply(lambda x: 0 if x == 'at_home' else (1 if x=='health' else (2 if x=='services' else (3 if x=='teacher' else 4))))
    print(studentData['b_fjob'].value_counts())

    #Assign the converted dataframe to new one for further use.Our new dataframe will be newStudentData.
    newStudentData=studentData.select_dtypes(include=[np.number])

    #Plotted the heatmap to show the correlation between variables.
    fig, ax = plt.subplots(figsize=(25,15))
    sns.heatmap(newStudentData.corr(),
                annot=True,
                linewidths=0.5,
                cmap="gnuplot"
            )
    plt.show()


   #Most impacting Feature Selection
    dfCorr = newStudentData.corr()
    
    #Getting the correlation of variables with G3(our target) and sorting
    corr_stacked = dfCorr.stack().loc['G3',:].sort_values(ascending=False)

    #extracted positively impacting and negatively impacting feature
    positive_feature=corr_stacked.head(10).drop('G3').index.tolist()
    negative_feature=corr_stacked.tail(9).index.tolist()    
    feature_list=positive_feature+negative_feature
    
    #Splitting the dataset for testing and Training 
    X_independent = newStudentData[feature_list]
    Y_dependent = newStudentData['G3']
    X_train, X_test, y_train, y_test = train_test_split(
    X_independent,Y_dependent , random_state=104,test_size=0.30, shuffle=True)

    #Fitting the model to linear regression
    regr = linear_model.LinearRegression()
    regr.fit(X_train, y_train)
    
    #input from the user is converted to a 2Darray and predict
    inputlistfinal=[inputlist]
    predictedGrade = regr.predict(inputlistfinal)
        
    return predictedGrade

    #We checked the accuracy score of the model and it is 84%. So now we removed it from the executing part.
    score=r2_score(y_test,predictedGrade)
    print("r2 socre is ",score)
    print("mean_sqrd_error is==",mean_squared_error(y_test,predictedGrade))
    print("root_mean_squared error of is==",np.sqrt(mean_squared_error(y_test,predictedGrade)))

    


    