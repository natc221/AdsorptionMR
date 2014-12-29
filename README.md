Adsroption Map Reduce
============

Adsorption algorithm implemented in Hadoop MapReduce, algorithm as seen in 

  Video Suggestion and Discovery for YouTube: Taking
  Random Walks Through the View Graph
  
from Google Research: 
http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/34407.pdf

To run executable jar, after setting up the appropriate Hadoop environment,
run jar with the command:

hadoop jar FriendRec.jar composite <input> <output> <numReducers> <intermediatePath> <intermediatePath2> <numIterations>

Input file format is an edge list, with a edge weight for each edge.
e.g.

1 2 0.5

1 3 0.3

4 2 1.0

Node names do not have to be integers, they are treated as strings.
