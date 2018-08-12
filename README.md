# README

Spark examples from workshops. 

For Scala Learning, refer https://github.com/nodesense/scala-workshop


## Source Code
 
 All the source code is located inside src folder
 within project root directory, data directory to contain all the data
 
 all the outputs goes into /output directory
 
 ## Bugs
 
  - FileUtils.rmdir not removing files
  - Please delete the output results folder
  
## Windows
    - Please ensure wintutil.exe in path
    - https://github.com/steveloughran/winutils
    - SET ENV HADOOP_HOME for c:\hadoop1.7.x
    - APPEND PATH with c:\hadoop1.7.x\bin
    
 ## To Run Streaming
 
    - Download and install node.js 8.x from https://nodejs.org/en/download/
    
    - open command prompt, go to project location
    - cd into node folder
    
  To run order streaming example,
  
  For word examples,
  
    > npm run words
    
  Ctrl + C to exit the program
  
  To run retail data about 45 MB
  
    > npm run retail
    
  
  to run apple stock market data
    
    > npm run stock
    
  to run birthdays,
  
    > npm run birthdays
    