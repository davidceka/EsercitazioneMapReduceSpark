javac -cp $(hadoop classpath) -d build/MaxFind src/MaxFind*.java
jar cvf MaxFind.jar -C build/MaxFind .
javac -cp $(hadoop classpath) -d build/WordCount src/WordCount*.java
jar cvf WordCount.jar -C build/WordCount .


