import psycopg2
import sys
import csv
import time

def createreflectionAssessments():
         
  try:
          conn=psycopg2.connect(database="currentdb", user="postgres", password="010305")
  except Exception as e:
          print (e)
# use a cursor to prepare for executing the sql. 
  cur=conn.cursor()
  sql="""
  SELECT
  core_experiences.name as experience_name,
  core_programs.name as program_name,
  project_projects.name as project_name,
  project_timelines.start_date as project_start_date,
  core_licenses.role,
  core_users.name as participant_name,
  core_enrolments.participant_id,
  core_enrolments.participant_email,
  core_teams.name as team_name,
  core_users.lastlog,
  public.core_users.activated_at,
  core_users.id,
  core_programs.id

  FROM 
  public.core_licenses,
  public.core_enrolments,
  public.core_users,
  public.core_programs,
  public.core_teams,
  public.core_team_members,
  public.core_experiences, 
  public.project_timelines,
  public.project_projects
  
  WHERE
  core_enrolments.license_id = core_licenses.id AND
  core_users.id = core_enrolments.user_id AND
  core_programs.id = core_enrolments.program_id AND
  core_teams.program_id = core_programs.id AND
  core_team_members.team_id = core_teams.id AND
  core_team_members.user_id = core_users.id AND
  core_experiences.id = core_programs.experience_id AND  
  project_timelines.project_id = project_projects.id AND
  project_timelines.program_id = core_programs.id
  order by core_teams.id
  """
  
  cur.execute(sql)
  time.sleep(2)
  
  rows=cur.fetchall()
  
  with open('/home/hadoop/Downloads/intersective_report/reflectionassessments.csv', 'wb') as csvfile:
 ####the number of comments (response)
   sql="""
select count(*)
from 
(SELECT 
  assess_assessment_submission_answers.comment, 
  assess_assessment_submissions.status, 
  assess_assessment_submissions.submitter_id, 
  assess_assessment_submissions.submitted, 
  assess_assessment_submissions.program_id, 
  assess_assessment_submissions.assessment_id, 
  assess_assessments.name, 
  assess_assessment_questions.name, 
  assess_assessment_questions.id as question_id
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions, 
  public.assess_assessment_submission_answers, 
  public.assess_assessment_questions
WHERE 
  assess_assessment_submissions.assessment_id = assess_assessments.id AND
  assess_assessment_submissions.id = assess_assessment_submission_answers.assessment_submission_id AND
  assess_assessment_questions.id = assess_assessment_submission_answers.assessment_question_id AND
  assess_assessment_questions.assessment_id = assess_assessments.id and assess_assessments.assessment_type='reflection') as k1
  group by submitter_id, program_id, assessment_id, question_id

  order by submitter_id;
  """        

   cur.execute(sql)
   k=0       
   rows_temp=cur.fetchall()

   for row_temp in rows_temp:

         if int(row_temp[0])>k:

             k=int(row_temp[0])
             
#########the number of reflections
             
   sql="""
select count(*)
from
(select k.submitter_id, k.program_id, k.assessment_id, count(*)
from 
(select submitter_id, program_id, assessment_id, question_id, count(*)
from 
(SELECT 
  assess_assessment_submission_answers.comment, 
  assess_assessment_submissions.status, 
  assess_assessment_submissions.submitter_id, 
  assess_assessment_submissions.submitted, 
  assess_assessment_submissions.program_id, 
  assess_assessment_submissions.assessment_id, 
  assess_assessments.name, 
  assess_assessment_questions.name, 
  assess_assessment_questions.id as question_id
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions, 
  public.assess_assessment_submission_answers, 
  public.assess_assessment_questions
WHERE 
  assess_assessment_submissions.assessment_id = assess_assessments.id AND
  assess_assessment_submissions.id = assess_assessment_submission_answers.assessment_submission_id AND
  assess_assessment_questions.id = assess_assessment_submission_answers.assessment_question_id AND
  assess_assessment_questions.assessment_id = assess_assessments.id and assess_assessments.assessment_type='reflection') as k1
  group by submitter_id, program_id, assessment_id, question_id
  order by submitter_id) as k
  group by k.submitter_id, k.program_id, k.assessment_id) as t
  group by t.submitter_id, t.program_id;
  """        

   cur.execute(sql)
   k1=0       
   rows_temp=cur.fetchall()

   for row_temp in rows_temp:

         if int(row_temp[0])>k1:

             k1=int(row_temp[0])
#########the number of questions

   sql="""
select count(*)
from 
(select submitter_id, program_id, assessment_id, question_id, count(*)
from 
(SELECT 
  assess_assessment_submission_answers.comment, 
  assess_assessment_submissions.status, 
  assess_assessment_submissions.submitter_id, 
  assess_assessment_submissions.submitted, 
  assess_assessment_submissions.program_id, 
  assess_assessment_submissions.assessment_id, 
  assess_assessments.name, 
  assess_assessment_questions.name, 
  assess_assessment_questions.id as question_id
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions, 
  public.assess_assessment_submission_answers, 
  public.assess_assessment_questions
WHERE 
  assess_assessment_submissions.assessment_id = assess_assessments.id AND
  assess_assessment_submissions.id = assess_assessment_submission_answers.assessment_submission_id AND
  assess_assessment_questions.id = assess_assessment_submission_answers.assessment_question_id AND
  assess_assessment_questions.assessment_id = assess_assessments.id and assess_assessments.assessment_type='reflection') as k1
  group by submitter_id, program_id, assessment_id, question_id
  order by submitter_id) as k
  group by k.submitter_id, k.program_id, k.assessment_id;
  """        

   cur.execute(sql)
   k2=0       
   rows_temp=cur.fetchall()

   for row_temp in rows_temp:

         if int(row_temp[0])>k2:

             k2=int(row_temp[0])
             
########################################################################     

#   print (rows_temp)
   spamwriter = csv.writer(csvfile, dialect='excel')   
   spamwriter.writerow(['experience_name']  + ['program_name'] + ['project_name']+ ['project_start_date'] +['role'] + ['participant_name'] + ['participant_id'] + \
                       ['participant_email']+ ['team_name'] + ['lastlog'] +['activated_at']+ \
                       (['reflection_name'] +['status']+['submitted_time']+(['reflection_question']+(['comments'])*k)*k2)* k1)

#   spamwriter.writerow(['id1']  + ['id2'] + \
#                    (['reflection_name'] +['status']+['submitted_time']+(['reflection_question']+(['comments'])*k)*k2)* k1)
###initilization

#   pass
   
   sql="""
SELECT 
  assess_assessment_submissions.submitter_id,
  assess_assessment_submissions.program_id,
  assess_assessment_submissions.status,  
  assess_assessment_submissions.submitted,  
  assess_assessment_submissions.assessment_id, 
  assess_assessments.name, 
  assess_assessment_questions.id as question_id,
  assess_assessment_questions.name, 
  assess_assessment_submission_answers.comment 
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions, 
  public.assess_assessment_submission_answers, 
  public.assess_assessment_questions
WHERE 
  assess_assessment_submissions.assessment_id = assess_assessments.id AND
  assess_assessment_submissions.id = assess_assessment_submission_answers.assessment_submission_id AND
  assess_assessment_questions.id = assess_assessment_submission_answers.assessment_question_id AND
  assess_assessment_questions.assessment_id = assess_assessments.id and assess_assessments.assessment_type='reflection'
 order by submitter_id, program_id, question_id, assessment_id;
  """        

   cur.execute(sql)
   time.sleep(2)
   rows1=cur.fetchall()
   #temp=[[] for r in range((((k+1)*k2+3)*k1+2)]
   temp=[['' for r in range((((k+1)*k2+3)*k1+2))]] # to avoid shallow copy
   temp_dict={}
         
   for row1 in rows1:

#                if  (row1[0]==row[11] and row1[1]==row[12]):
                 if row1[0] not in temp_dict:
                       
                       temp[(len(temp)-1)][0]=row1[0]#usr_id
                       temp[(len(temp)-1)][1]=row1[1] #program_id
                       temp[(len(temp)-1)][2]=row1[5] #assessment_name                       
                       temp[(len(temp)-1)][3]=row1[2] #status
                       temp[(len(temp)-1)][4]=row1[3] #submitted
                       #temp[i][4]=row1[4] #assessment_id
                       a_row1=row1[7].replace(';', '.')
                       temp[(len(temp)-1)][5]=a_row1 #question_name
                       try: 
                           a_row1=row1[8].replace(';', '.')
                           temp[(len(temp)-1)][6]=a_row1 
                       except Exception as e:
                           temp[(len(temp)-1)][6]=row1[8]                                                    #comments
                       temp_dict[row1[0]]={}
                       temp_dict[row1[0]][row1[1]]={}
                       temp_dict[row1[0]][row1[1]][row1[4]]={}
                       temp_dict[row1[0]][row1[1]][row1[4]][row1[6]]=1
                       temp.append(['' for r in range((((k+1)*k2+3)*k1+2))])
        
                       
                 else:
                   
                        if row1[1] not in temp_dict[row1[0]].keys():
                            temp[(len(temp)-1)][0]=row1[0]#usr_id
                            temp[(len(temp)-1)][1]=row1[1] #program_id
                            temp[(len(temp)-1)][2]=row1[5] #assessment_name                       
                            temp[(len(temp)-1)][3]=row1[2] #status
                            temp[(len(temp)-1)][4]=row1[3] #submitted
                                     #temp[i][4]=row1[4] #assessment_id 
                            a_row1=row1[7].replace(';', '.')
                            temp[(len(temp)-1)][5]=a_row1 #question_name
                            try: 
                                a_row1=row1[8].replace(';', '.')
                                temp[(len(temp)-1)][6]=a_row1 
                            except Exception as e:
                                temp[(len(temp)-1)][6]=row1[8] 
                            temp_dict[row1[0]][row1[1]]={}
                            temp_dict[row1[0]][row1[1]][row1[4]]={}
                            temp_dict[row1[0]][row1[1]][row1[4]][row1[6]]=1
                            temp.append(['' for r in range((((k+1)*k2+3)*k1+2))])
                          
                        else: #usr,program same
                            if row1[4] not in temp_dict[row1[0]][row1[1]].keys(): # belong to another reflection
                                p=len(temp_dict[row1[0]][row1[1]].keys())
                                p=p*((k+1)*k2+3)
                                temp[(len(temp)-2)][2+p]=row1[5] #assessment_name                       
                                temp[(len(temp)-2)][3+p]=row1[2] #status
                                temp[(len(temp)-2)][4+p]=row1[3] #submitted
                                     #temp[i][4]=row1[4] #assessment_id 
                                a_row1=row1[7].replace(';', '.')     
                                temp[(len(temp)-2)][5+p]=a_row1 #question_name
                                try: 
                                    a_row1=row1[8].replace(';', '.')
                                    temp[(len(temp)-2)][6+p]=a_row1 #question_id
                                except Exception as e:
                                    temp[(len(temp)-2)][6+p]=row1[8]
                                temp_dict[row1[0]][row1[1]][row1[4]]={}
                                temp_dict[row1[0]][row1[1]][row1[4]][row1[6]]=1
                                 
                            else: # usr, progarm, assessment_name same
                              if row1[6] not in temp_dict[row1[0]][row1[1]][row1[4]].keys():
                                    p=len(temp_dict[row1[0]][row1[1]].keys())
                                    p1=len(temp_dict[row1[0]][row1[1]][row1[4]].keys())
                                    p=(p-1)*((k+1)*k2+3)
                                    p1=(k+1)*p1
                                     #temp[i][4]=row1[4] #assessment_id 
                                    a_row1=row1[7].replace(';', '.') 
                                    temp[(len(temp)-2)][5+p1+p]=a_row1 #question_name
                                    try: 
                                       a_row1=row1[8].replace(';', '.')
                                       temp[(len(temp)-2)][6+p1+p]=a_row1 #comments
                                    except Exception as e:
                                       temp[(len(temp)-2)][6+p1+p]=row1[8]   
                                    temp_dict[row1[0]][row1[1]][row1[4]][row1[6]]=1
                                   # print (temp_dict[row1[0]][row1[1]][row1[4]][row1[6]])
                                   
                              else: # usr, progarm, assessment_name, question same
                                    p=len(temp_dict[row1[0]][row1[1]].keys())
                                    p1=len(temp_dict[row1[0]][row1[1]][row1[4]].keys())                                    
                                    p=(p-1)*((k+1)*k2+3)
                                    p1=(p1-1)*(k+1)
                                    p2=temp_dict[row1[0]][row1[1]][row1[4]][row1[6]]
                                    temp_dict[row1[0]][row1[1]][row1[4]][row1[6]]=p2+1
                                    #temp[i][4]=row1[4] #assessment_id 
                                    #temp[(len(temp)-2)][5+(p2+1)*p1+p]=row1[7] #question_name
                                    try:
                                        a_row1=row1[8].replace(';', '.')
                                        temp[(len(temp)-2)][6+p2+p1+p]=a_row1 #question_id
                                    except Exception as e:
                                        temp[(len(temp)-2)][6+p2+p1+p]=row1[8]
                                       
   
#    print (temp)                                
############################ 
   for row in rows:
        for temp1 in temp:           
            if  (temp1[0]==row[11] and temp1[1]==row[12]):                       
                      a=len(row)
                      temp2=list(row)
                                            
                      for m in range (a): 
                          temp2.insert((a+m), temp1[2+m])                    
                       
                      row=tuple(temp2)
                      temp3=list(row)
                      del temp3[11:13] 
                      row=tuple(temp3)
                      spamwriter = csv.writer(csvfile, dialect='excel')
                      spamwriter.writerow(row) 

#    for temp1 in temp:                                 
#                                           
#                        row=tuple(temp1)
#                        spamwriter = csv.writer(csvfile, dialect='excel')
#                        spamwriter.writerow(row)
   csvfile.close 

   conn.close

#######################csv read##################################################
##     with open('student_engagement.csv', 'rb') as csvfile:
##
##        spamreader = csv.reader(csvfile, dialect='excel')
##        for row in spamreader:
##           print ', '.join(row)
