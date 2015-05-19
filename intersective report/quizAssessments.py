import psycopg2
import sys
import csv
import time

def createquizAssessments():
         
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
  
  with open('/home/hadoop/Downloads/intersective_report/quizassessments.csv', 'wb') as csvfile:

   sql="""
select count(*)
from
(select m.submitter_id, n.program_id, n.name as assessment_name, m.status, n.submitted, m.attempt_time as attempts_number, n.score
from 
((SELECT  
  assess_assessment_submissions.submitter_id, 
  assess_assessments.id as assessment_id, assess_assessment_submissions.status, 
  count(*) as attempt_time, max(assess_assessment_submissions.submitted) as latest_submitted_time
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions
WHERE 
assess_assessment_submissions.assessment_id = assess_assessments.id 
and assess_assessments.assessment_type='quiz' and assess_assessment_submissions.status='submitted'

group by  assess_assessment_submissions.submitter_id, 
  assess_assessments.id, assess_assessment_submissions.status

 order by assess_assessment_submissions.submitter_id) as m

join

 (SELECT  
  assess_assessment_submissions.submitter_id, assess_assessment_submissions.program_id, 
  assess_assessments.id as assessment_id, assess_assessment_submissions.score, 
  assess_assessments.name,
  assess_assessment_submissions.submitted
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions
WHERE 
assess_assessment_submissions.assessment_id = assess_assessments.id 
and assess_assessments.assessment_type='quiz' and assess_assessment_submissions.status='submitted') as n

on m.assessment_id=n.assessment_id and m.submitter_id=n.submitter_id and m.latest_submitted_time=n.submitted)

union 

(SELECT  
  assess_assessment_submissions.submitter_id, 
  assess_assessment_submissions.program_id,
  assess_assessments.name as assessment_name, assess_assessment_submissions.status, 
  assess_assessment_submissions.submitted,
    count(*),
  assess_assessment_submissions.score
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions
WHERE 
assess_assessment_submissions.assessment_id = assess_assessments.id 
and assess_assessments.assessment_type='quiz' and assess_assessment_submissions.status!='submitted'

group by  assess_assessment_submissions.submitter_id, 
  assess_assessments.id, assess_assessment_submissions.status, assess_assessment_submissions.submitted, 
  assess_assessment_submissions.score,assess_assessment_submissions.program_id, assess_assessments.name

 order by assess_assessment_submissions.submitter_id)

 order by submitter_id) as k
 group by k.submitter_id, k.program_id
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
                       (['assessment_name'] +['status']+['submitted_time']+['attempts_number']+['score'])* k)              
 
   for row in rows:

        sql="""
(select m.submitter_id, n.program_id, n.name as assessment_name, m.status, n.submitted, m.attempt_time as attempts_number, n.score
from 
(SELECT  
  assess_assessment_submissions.submitter_id, 
  assess_assessments.id as assessment_id, assess_assessment_submissions.status, 
  count(*) as attempt_time, max(assess_assessment_submissions.submitted) as latest_submitted_time
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions
WHERE 
assess_assessment_submissions.assessment_id = assess_assessments.id 
and assess_assessments.assessment_type='quiz' and assess_assessment_submissions.status='submitted'

group by  assess_assessment_submissions.submitter_id, 
  assess_assessments.id, assess_assessment_submissions.status

 order by assess_assessment_submissions.submitter_id) as m

join

 (SELECT  
  assess_assessment_submissions.submitter_id, assess_assessment_submissions.program_id, 
  assess_assessments.id as assessment_id, assess_assessment_submissions.score, 
  assess_assessments.name,
  assess_assessment_submissions.submitted
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions
WHERE 
assess_assessment_submissions.assessment_id = assess_assessments.id 
and assess_assessments.assessment_type='quiz' and assess_assessment_submissions.status='submitted') as n

on m.assessment_id=n.assessment_id and m.submitter_id=n.submitter_id and m.latest_submitted_time=n.submitted)

union 

(SELECT  
  assess_assessment_submissions.submitter_id, 
  assess_assessment_submissions.program_id,
  assess_assessments.name as assessment_name, assess_assessment_submissions.status, 
  assess_assessment_submissions.submitted,
    count(*),
  assess_assessment_submissions.score
FROM 
  public.assess_assessments, 
  public.assess_assessment_submissions
WHERE 
assess_assessment_submissions.assessment_id = assess_assessments.id 
and assess_assessments.assessment_type='quiz' and assess_assessment_submissions.status!='submitted'

group by  assess_assessment_submissions.submitter_id, 
  assess_assessments.id, assess_assessment_submissions.status, assess_assessment_submissions.submitted, 
  assess_assessment_submissions.score,assess_assessment_submissions.program_id, assess_assessments.name

 order by assess_assessment_submissions.submitter_id)

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
