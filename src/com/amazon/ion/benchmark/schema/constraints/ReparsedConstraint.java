package com.amazon.ion.benchmark.schema.constraints;
/*
This interface is the abstraction of all constraints. It will be implemented by different constraint classes which have different domain knowledge.
After parsing the type definition, all constraints will be packed into a HashMap<String constraintName, ReparseConstraint parsedConstraintValue>.
The ReparsedConstraint will be cast into specific constraint based on which instance it represents.
*/
public interface ReparsedConstraint {

}
