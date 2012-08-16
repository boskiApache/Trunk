A = LOAD 'names.txt' 
USING PigStorage();
help
B = A;
STORE B INTO 'names2.txt' USING PigStorage();

