import psycopg2
import sys
import csv
import time

def createmoderatedAssessments():
         
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
  
  with open('/home/hadoop/Downloads/intersective_report/moderatedassessments.csv', 'wb') as csvfile:

   sql="""
select count (*)
from 
(SELECT  
  assess_assessment_submissions.submitter_id, 
  assess_assessments.name, 
  assess_assessments.id,
  assess_assessment_submissions.status, 
  assess_assessment_submissions.submitted as submitted_time, 
  assess_assessment_submissions.program_id,
  assess_assessment_submissions.score, 
  assess_assessment_submissions.moderated_score 
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions
WHERE 
assess_assessment_submissions.assessment_id = assess_assessments.id) as kk1
left join 
(SELECT  
  assess_assessments.id, 
  assess_assessment_submissions.submitter_id, 
  assess_assessment_submissions.status, 
  assess_assessment_submissions.submitted, 
  assess_assessment_reviews.reviewer_id, 
  core_users.name,
  assess_assessment_reviews.created, 
  assess_assessment_reviews.modified
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions, 
  public.assess_assessment_reviews,
  public.core_users
WHERE 
  assess_assessment_submissions.assessment_id = assess_assessments.id AND
  assess_assessment_submissions.id = assess_assessment_reviews.assessment_submission_id AND
  assess_assessment_reviews.assessment_id = assess_assessments.id AND core_users.id=assess_assessment_reviews.reviewer_id) as kk2

on kk1.submitter_id=kk2.submitter_id and kk1.id=kk2.id and kk1.status=kk2.status

Group by kk1.submitter_id, kk1.program_id
  """        

   cur.execute(sql)
   k=0       
   rows_temp=cur.fetchall()

   for row_temp in rows_temp:

         if int(row_temp[0])>k:

             k=int(row_temp[0])
     

#   print (rows_temp)
   spamwriter = csv.writer(csvfile, dialect='excel')   
   spamwriter.writerow(['experience_name']  + ['program_name'] + ['project_name']+ ['project_start_date'] +['role'] + ['participant_name'] + ['participant_id'] + \
                       ['participant_email']+ ['team_name'] + ['lastlog'] +['activated_at']+ \
                       (['assessment_name'] +['status']+['submitted_time']+['score']+['moderated_score']+['reviewer_name']+['review_created_time']+['review_modified_time'])* k)              
 
   for row in rows:

        sql="""
select kk1.submitter_id, kk1.program_id, kk1.name as assessment_name, kk1.status, kk1.submitted_time, 
kk1.score, kk1.moderated_score, kk2.reviewer_id, kk2.name as reviewer_name, kk2.created as review_created_time, 
kk2.modified as review_modified_time
from 
(SELECT  
  assess_assessment_submissions.submitter_id, 
  assess_assessments.name, 
  assess_assessments.id,
  assess_assessment_submissions.status, 
  assess_assessment_submissions.submitted as submitted_time, 
  assess_assessment_submissions.program_id,
  assess_assessment_submissions.score, 
  assess_assessment_submissions.moderated_score 
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions
WHERE 
assess_assessment_submissions.assessment_id = assess_assessments.id) as kk1
left join 
(SELECT  
  assess_assessments.id, 
  assess_assessment_submissions.submitter_id, 
  assess_assessment_submissions.status, 
  assess_assessment_submissions.submitted, 
  assess_assessment_reviews.reviewer_id, 
  core_users.name,
  assess_assessment_reviews.created, 
  assess_assessment_reviews.modified
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions, 
  public.assess_assessment_reviews,
  public.core_users
WHERE 
  assess_assessment_submissions.assessment_id = assess_assessments.id AND
  assess_assessment_submissions.id = assess_assessment_reviews.assessment_submission_id AND
  assess_assessment_reviews.assessment_id = assess_assessments.id AND core_users.id=assess_assessment_reviews.reviewer_id) as kk2

on kk1.submitter_id=kk2.submitter_id and kk1.id=kk2.id and kk1.status=kk2.status
  """        

        cur.execute(sql)
        
        rows1=cur.fetchall()
        
        for row1 in rows1:

                if  (row1[0]==row[11] and row1[1]==row[12]):

                      a=len(row)
                      temp=list(row)
                      temp.insert(a, row1[2])                    
                      temp.insert((a+1), row1[3])
                      temp.insert((a+2), row1[4])
                      temp.insert((a+3), row1[5])
                      temp.insert((a+4), row1[6])
                      temp.insert((a+5), row1[8])
                      temp.insert((a+6), row1[9])
                      temp.insert((a+7), row1[10])
                      row=tuple(temp)
                      
        temp=list(row)
        del temp[11:13] 
        row=tuple(temp)
        spamwriter = csv.writer(csvfile, dialect='excel')
        spamwriter.writerow(row) 
  csvfile.close 

  conn.close

#######################csv read##################################################
##     with open('student_engagement.csv', 'rb') as csvfile:
##
##        spamreader = csv.reader(csvfile, dialect='excel')
##        for row in spamreader:
##           print ', '.join(row)
