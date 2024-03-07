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
1. Filter as early as possible: Select operators will be used before join operators if WHERE clause is detected. This can create smaller tuples and reduce intermediate results, or project out redundant attributes.
2. Break complex predicates and push down. In class MyExpressionDeParser, the current tuple will be neglected if a false predicate is found before others.
3. Projection Push-down: I've built multiple constructors for Projection Operators, each with a different child operator to deal with different situations. E.g., If join is detected, it will try to project needed attributes only before join.  