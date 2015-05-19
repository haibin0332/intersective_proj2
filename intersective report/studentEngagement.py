import psycopg2
import sys
import csv
import time

def createstudentEngagement():
         
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
  stat.lastest_stat,
  stat.stat_id,
  stat.stat_value,
  stat.team_average,
  public.core_users.activated_at,
  core_users.id,
  core_programs.id,
  core_teams.id
  FROM 
  public.core_licenses,
  public.core_enrolments,
  public.core_users,
  public.core_programs,
  public.core_teams,
  public.core_team_members,
  public.core_experiences, (select user_latest_stat.user_id, user_latest_stat.team_id,
  user_latest_stat.stat_id, user_latest_stat.lastest_stat, user_latest_stat.stat_value, latest_average.team_average
  from (select temp1.team_id, temp1.user_id, temp1.stat_id, temp2.stat_value, temp1.lastest_stat from (SELECT
  core_team_stats.team_id, core_team_stats.user_id, core_team_stats.stat_id, max(core_team_stats.created) as lastest_stat FROM
  public.core_team_stats group by core_team_stats.user_id, core_team_stats.stat_id, core_team_stats.team_id
  order by core_team_stats.team_id, core_team_stats.stat_id) as temp1, (SELECT
  core_team_stats.user_id, core_team_stats.stat_id, core_team_stats.stat_value, core_team_stats.created FROM
  public.core_team_stats) as temp2
  where temp1.user_id=temp2.user_id and temp1.stat_id=temp2.stat_id and temp1.lastest_stat=temp2.created) as user_latest_stat left join
(select core_team_stats.team_id, core_team_stats.stat_id, AVG (core_team_stats.stat_value::integer) as team_average from public.core_team_stats join
(SELECT core_team_stats.user_id, core_team_stats.stat_id, max(core_team_stats.created) as latest_created
FROM public.core_team_stats group by core_team_stats.user_id, core_team_stats.stat_id) as latest
on public.core_team_stats.created = latest.latest_created and
public.core_team_stats.user_id = latest.user_id and public.core_team_stats.stat_id=latest.stat_id
group by core_team_stats.team_id, core_team_stats.stat_id
order by team_id) as latest_average
on user_latest_stat.team_id = latest_average.team_id and user_latest_stat.stat_id = latest_average.stat_id
order by user_latest_stat.team_id, user_latest_stat.stat_id) as stat,
public.project_timelines,
public.project_projects
WHERE
core_enrolments.license_id = core_licenses.id AND
core_users.id = core_enrolments.user_id AND
core_programs.id = core_enrolments.program_id AND
core_teams.program_id = core_programs.id AND
core_team_members.team_id = core_teams.id AND
core_team_members.user_id = core_users.id AND
core_experiences.id = core_programs.experience_id AND stat.team_id = core_teams.id AND  project_timelines.project_id = project_projects.id AND
project_timelines.program_id = core_programs.id
order by core_teams.id, stat.stat_id
  """
  
  cur.execute(sql)
  time.sleep(4)
  
  rows=cur.fetchall()
  
  with open('/home/hadoop/Downloads/intersective_report/studentengagement.csv', 'wb') as csvfile:

   sql="""
select count (*)
from 
(SELECT  
  core_users.id as user_id,
  core_programs.id as program_id, 
  core_teams.id as team_id
FROM 
  public.core_licenses, 
  public.core_enrolments, 
  public.core_programs, 
  public.core_teams, 
  public.core_team_members, 
  public.core_users
WHERE 
  core_enrolments.license_id = core_licenses.id AND
  core_teams.program_id = core_programs.id AND
  core_team_members.user_id = core_users.id AND
  core_team_members.team_id = core_teams.id AND
  core_users.id = core_enrolments.user_id AND core_licenses.role='participant'
Group by core_programs.id, core_users.id, core_teams.id) as kk1
join 
(SELECT  
  core_users.name as mentor_name,
  core_users.email as mentor_email,
  core_programs.id as program_id, 
  core_teams.id as team_id
FROM 
  public.core_licenses, 
  public.core_enrolments, 
  public.core_programs, 
  public.core_teams, 
  public.core_team_members, 
  public.core_users
WHERE 
  core_enrolments.license_id = core_licenses.id AND
  core_teams.program_id = core_programs.id AND
  core_team_members.user_id = core_users.id AND
  core_team_members.team_id = core_teams.id AND
  core_users.id = core_enrolments.user_id AND core_licenses.role!='participant'
Group by core_programs.id, core_users.id, core_teams.id, core_users.email) as kk2
on kk1.program_id=kk2.program_id and kk1.team_id=kk2.team_id
Group by kk1.user_id, kk1.program_id, kk1.team_id
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
                       ['participant_email']+ ['team_name'] + ['lastlog'] +['lastest_stat'] + ['stat_id'] + ['stat_value'] + ['team_average'] +['activated_at']+ (['mentor_name'] +['mentor_email'])* k)              
 
   for row in rows:

        sql="""
select kk1.user_id, kk1.program_id, kk1.team_id, kk2.mentor_name, kk2.mentor_email
from 
(SELECT  
  core_users.id as user_id,
  core_programs.id as program_id, 
  core_teams.id as team_id
FROM 
  public.core_licenses, 
  public.core_enrolments, 
  public.core_programs, 
  public.core_teams, 
  public.core_team_members, 
  public.core_users
WHERE 
  core_enrolments.license_id = core_licenses.id AND
  core_teams.program_id = core_programs.id AND
  core_team_members.user_id = core_users.id AND
  core_team_members.team_id = core_teams.id AND
  core_users.id = core_enrolments.user_id AND core_licenses.role='participant'
Group by core_programs.id, core_users.id, core_teams.id) as kk1
join 
(SELECT  
  core_users.name as mentor_name,
  core_users.email as mentor_email,
  core_programs.id as program_id, 
  core_teams.id as team_id
FROM 
  public.core_licenses, 
  public.core_enrolments, 
  public.core_programs, 
  public.core_teams, 
  public.core_team_members, 
  public.core_users
WHERE 
  core_enrolments.license_id = core_licenses.id AND
  core_teams.program_id = core_programs.id AND
  core_team_members.user_id = core_users.id AND
  core_team_members.team_id = core_teams.id AND
  core_users.id = core_enrolments.user_id AND core_licenses.role!='participant'
Group by core_programs.id, core_users.id, core_teams.id, core_users.email) as kk2
on kk1.program_id=kk2.program_id and kk1.team_id=kk2.team_id
Group by kk1.user_id, kk1.program_id, kk1.team_id, kk2.mentor_name, kk2.mentor_email
  """        

        cur.execute(sql)
        
        rows1=cur.fetchall()
        
        for row1 in rows1:

                if  (row1[0]==row[15] and row1[1]==row[16] and row1[2]==row[17]):

                      a=len(row)
                      b=a+1
                      temp=list(row)
                      temp.insert(a, row1[3])                    
                      temp.insert(b, row1[4])
                      row=tuple(temp)
                      
        temp=list(row)
        del temp[15:18] 
        row=tuple(temp)
        spamwriter = csv.writer(csvfile, dialect='excel')
          #spamwriter.writerow((['experience_name']+ ['experience_id'])* k \
          #                    + ['Baked Beans'])
        spamwriter.writerow(row) 
  csvfile.close 

  conn.close

#######################csv read##################################################
##     with open('student_engagement.csv', 'rb') as csvfile:
##
##        spamreader = csv.reader(csvfile, dialect='excel')
##        for row in spamreader:
##           print ', '.join(row)
