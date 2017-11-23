# meetup-akka-streams

Tutorial about Akka-Streams basic using Flow and Graphs DSL

1. Example #1 is Twitter tweets stream through the Akka-Http:
   
   - You need to create a src/main/resources/reference.conf file with Twitter API secret keys/params. There are 4 properties expected:
   
           twitter {  
             consumerKey = ...    
             consumerSecret = ...    
             accessToken = ...    
             accessTokenSecret = ...     
           }
           
    - Build and publish oauth-headers artifact, using below instruction:
        + git clone https://github.com/3drobotics/cloud-oauth-headers.git
        - sbt publish-local
                             
2.  Example #2 is the hypothetical Stock Exchange order processor which: 

    Translates receiving model into the internal model and then persists it in database for further processing




