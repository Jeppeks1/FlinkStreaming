### Overview

The code base contains two different implementations of the k-Nearest Neighbor search. The main methods of each implementation serve as the entry point for the Flink cluster and can be found in /src/main/scala.

### Running the code

To run and test your application locally, you can execute `sbt run` then select the main class that contains the Flink job. 

You can also package the application into a fat jar with `sbt assembly`, then submit it as usual, with something like: 

```
flink run -c org.example.WordCount /path/to/your/project/jar/mainRunner.jar
```

You can also run your application from within IntelliJ:  select the classpath of the 'mainRunner' module in the run/debug configurations.
Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the "Use classpath of module" dropbox. 

Four .jar files should be contained in the hand-in as part of this project. The .jar files can be submitted to a flink cluster using the flink run command above. See the main methods for a list of expected parameters that should be passed to the methods.
