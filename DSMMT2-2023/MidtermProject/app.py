import streamlit as st
import numpy as np
import pandas as pd
from StudentScorePredictor import *

#used streamlit for the UI part to show the results.
def main():
    """Very Simple Linear Regression App"""
    #Title of the page
    st.title('Student Score Predictor')
    html_templ = """<div style="background-color:grey;padding:10px;"> <h3 style="color:cyan">Very Simple Score Prediction Web App for Students</h3> </div>"""
    st.markdown(html_templ,unsafe_allow_html=True)
    #Menu
    activity = ["About","Score Prediction","Team"]
    choice = st.sidebar.selectbox("Menu",activity)

    if choice == 'About':
        st.subheader("About")
        st.markdown("""
			 
            Simple Student Score Predictor App


            It is a very simple Score predicting application where students can check their final score based on different criteria. We can use machine learning for the student score prediction task so that instructors can help students prepare for topics where student grades were predicted low.We trained this model based on the data of students in a portuguese school. Other than academic features, we had covered other external factors that may impact the score of a student. The users are requested to enter the details for Predicting their future scores. It has an accuracy rate of 84%.
			""")
    
    if choice == 'Team' :
        st.subheader("Group 10")
        st.markdown("""
            Karthi Kuthalingam\n
            Megha Chauhan\n
            Abbas Ismail\n
            Enrique Gonzalez Zepeda\n
            Gitik Kaushik
            """)
        
    if choice == 'Score Prediction':
        #Questionaire to fill by the user to get their grade predicted.
        st.title("Complete the questionaire")        
        name  = st.text_input("Student Name")
        age = st.slider("Choose age",min_value=0, max_value=25,step=1)
        highereducation = st.radio("Interested for higher education?", ('Yes','No'))
        extraclass = st.radio("Do you attend any paid extra classes?", ('Yes','No'))
        studytimeinhrs=st.number_input('How many study hours in a week?',min_value=0, max_value=15, step=1)
        internetaccess = st.radio("Do you have Internet connection", ('Yes','No'))
        schoolsupp = st.radio("Does your school supports?", ('Yes','No'))
        traveltimeinhrs=st.number_input('Travel time to school in hours',min_value=0, max_value=15, step=1)
        failed=st.number_input('Number of subject failed before',min_value=0, max_value=15, step=1)
        score_G1=st.number_input("First period score",min_value=0, max_value=20, step=1)
        score_G2=st.number_input("Second period score",min_value=0, max_value=20, step=1)
        outingtimeinhrs=st.number_input('Hours spend with Friends',min_value=0, max_value=20, step=1)
        romance = st.radio("Are you in a Relationship?", ('Yes','No'))
        healthrating = st.slider("Rate your health condition",min_value=0, max_value=5, step=1)
        home=st.selectbox("Where do you live?",("Urban", "Rural"))
        fammembers=st.selectbox("Family Members",("Greater than 3", "Less or equal to 3"))
        parentstatus=st.selectbox("Parents Cohabitation status",("Lived Together", "Lived Apart"))
        fatheredu=st.selectbox("Father Education level",("No Education", "Primary(4th grade)","5th to 9th grade","Secondary Education","Higher Education"))
        motheredu=st.selectbox("Mother Education Level",("No Education", "Primary(4th grade)","5th to 9th grade","Secondary Education","Higher Education"))

        #Preparing a list of user variable to pass it to the function to get the future score predicted.
        if st.button("Predict Score"):
            inputlist=[]
            medu=lambda motheredu: 0 if motheredu == 'No Education' else (1 if motheredu=='Primary(4th grade)' else (2 if motheredu=='5th to 9th grade' else (3 if motheredu=='Secondary Education' else 4)))
            fedu=lambda fatheredu: 0 if fatheredu == 'No Education' else (1 if fatheredu=='Primary(4th grade)' else (2 if fatheredu=='5th to 9th grade' else (3 if fatheredu=='Secondary Education' else 4)))
            higher=lambda highereducation: 0 if highereducation == 'No' else 1
            xclass=lambda extraclass: 0 if extraclass == 'No' else 1
            internet=lambda internetaccess: 0 if internetaccess == 'No' else 1
            pstatus=lambda parentstatus: 0 if parentstatus == 'Lived Together' else 1
            fam=lambda fammembers: 0 if fammembers == 'Less or equal to 3' else 1
            school=lambda schoolsupp: 0 if schoolsupp == 'No' else 1
            address=lambda home: 0 if home == 'Urban' else 1
            relation=lambda romance: 0 if romance == 'No' else 1
            inputlist.append(score_G2)
            inputlist.append(score_G1)
            inputlist.append(medu(motheredu))
            inputlist.append(higher(highereducation))
            inputlist.append(fedu(fatheredu))
            inputlist.append(xclass(extraclass))
            inputlist.append(internet(internetaccess))
            inputlist.append(studytimeinhrs)
            inputlist.append(pstatus(parentstatus))
            inputlist.append(healthrating)
            inputlist.append(fam(fammembers))
            inputlist.append(school(schoolsupp))
            inputlist.append(address(home))
            inputlist.append(traveltimeinhrs)
            inputlist.append(relation(romance))
            inputlist.append(outingtimeinhrs)
            inputlist.append(age)
            inputlist.append(failed)
            
            # Calling the predicting function and return the result.
            predictG3=gradePrediction(inputlist)

            #Show the result in the page. Since it is all based on accuracy level, not removed the decimal places , but rounded to 3 places.
            st.subheader('Predicted Score of {} will be {}/20'.format(name , round(predictG3[0], 3)))        
         



if __name__ == '__main__':
    main()