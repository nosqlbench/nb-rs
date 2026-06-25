* optimizer improvements
  * initial space probe from centroid
  * PCA sort
  * axis prioritization
* Support rates in string form as count/time in fractional, small and large
* provide a second way to have the daemon query (within a phase)
* support displays when there is more than one active phase
* generalize execution shell to make use of wrappers at any node
* support JSON templating authentically
* bring in parser-aware yaml parsing for bare literals vs quotes, retaining 
  Value types
* allow mixed-cursor usage in a scope, as long as the consumption stride 
  aligns for each read point
* Algebraically speaking, an empty partition should not be a problem for 
  iteration, because iterating over an empty set is just doing nothing, 
  and starting and stopping a deamon thread is meaningless if done for as 
  little time as possible.
* "daemon no-spam after a for-loop halt" smells ... too special 
