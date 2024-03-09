# My In-Memory-Database
## UoE_ADBS
Author: He-Yi Lin

## Explanation:
# 1. Logic of join conditions on WHERE clause.
The mechanism of join operator is specifically explained in comments of class JoinOperator.
The mechanism of extracting join conditions from the WHERE clause is specifically explained in the comments of class myExpressionDePaser.
In this project, a proper-designed data structure tuple in class Tuple contributes a lot.
The tuple contains most required information to match extracted join condition information, which relieves pressure of other components in the system to tracing necessary information.
The tuple structure can be found in comments of class Tuple. Generally speaking, the operating mechanism of this database system is explained by the comments in code.

# 2. Optimization Rules
1. Filter as early as possible: 
- Based on RA equivalences, we can cascade selections and commute them.
- Select operators will be used before join operators if WHERE clause is detected. 
- This can create smaller tuples and reduce intermediate results, or project out redundant attributes.
2. Break complex predicates and push down: 
- We can break a conjunction of predicates into nested layers, for example, the whole expression will be false if one predicate is found false in a conjunction.
- In class MyExpressionDeParser, the current tuple will be neglected as long as a false predicate is found.
- This can reduce time on validating a bulk Where Expression.
3. Projection Push-down: 
- We can only project attributes which are needed in query plan as early as possible to create smaller tuples and intermediate result.
- I've built multiple constructors for Projection Operators, each with a different child operator to deal with different situations. 
- E.g., If join is detected, it will try to project only needed attributes after a selectOperator or scanOperator before join.  