#!/usr/bin/env python

#	Copyright 2015 Intersective code by Haibin Zhang
#
# generate sql report for intersective.
#

#import psycopg2
#import sys
import studentEngagement
import moderatedAssessments
import quizAssessments
import reflectionAssessments

if __name__ == '__main__':

 while True:

  print('')
  print('#########################')
  print('#   Report Generation   #')
  print('#########################')
  print('')

  print('')
  print('##########################################################################################')
  print('#   Student engagement, Moderated assessments, Quiz assessments, Reflection assessments  #')
  print('##########################################################################################')
  print('')
   
  output_options=raw_input('Please enter the name of report: ')

########################################################################################################output_options
  
  if output_options == 'Student engagement':

         studentEngagement.createstudentEngagement()


         print ('Finish Processing')
         print ('')

         break

  elif  output_options == 'Moderated assessments':

         moderatedAssessments.createmoderatedAssessments()


         print ('Finish Processing')
         print ('')       
         break
        
  elif  output_options == 'Quiz assessments':

         quizAssessments.createquizAssessments()

         
         print ('Finish Processing')
         print ('')       
         break
        
  elif  output_options == 'Reflection assessments':

         reflectionAssessments.createreflectionAssessments()


         print ('Finish Processing')
         print ('')       
         break         
######################################################################################################// output_options
        
  else:
 
         print('Error input, please reenter.')
         
