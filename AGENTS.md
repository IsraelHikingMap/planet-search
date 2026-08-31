# AGENTS.md

Conventions for AI agents working in this repository. They apply to code an
agent writes or edits, not to a sweep of code it was not asked to touch.

## Keep answers short

Answer in as few words as the question needs. State the conclusion, and the
evidence for it if it is not obvious — not the route taken to get there. No
recaps of what was just done, no tables where a sentence works, no closing
summary of a summary.

Length has to be earned: a measurement that changes the decision is worth a
line, a list of every file touched is not.

## Comments belong on the method, not inside it

Explain a method in its javadoc. When a branch, a condition or a loop needs
justifying, that justification goes at the top of the method as part of the
contract the javadoc already states — a reader should understand why the method
does what it does before reading a line of its body, rather than reassembling
the reasoning from remarks scattered through it.

Prefer this:

```java
/**
 * Remembers a relation's settlement node members, so their location and place
 * kind can be read back when the relation itself comes past.
 *
 * A place relation is anchored by either node it names; a boundary that is not
 * tagged as a place takes its identity from its label node alone, since
 * admin_centre names the seat of government — for a regional council, a
 * different settlement from the council itself.
 */
void recordRelationIfNeeded(OsmElement.Relation relation) {
  for (var member : relation.members()) {
    boolean anchorsThisRelation = isLabelNodeMember(member)
        || (isPlaceRelation && isAdminCentreNodeMember(member));
```

over this:

```java
void recordRelationIfNeeded(OsmElement.Relation relation) {
  for (var member : relation.members()) {
    // A place relation is anchored by either settlement node it names, but a
    // boundary that is not itself tagged as a place takes its identity from
    // its label node only: admin_centre is the seat of government, which for
    // a regional council is a different settlement from the council itself.
    boolean anchorsThisRelation = isLabelNodeMember(member)
        || (isPlaceRelation && isAdminCentreNodeMember(member));
```

If the explanation is too long to sit in the javadoc, that is a sign the method
is doing too much — split it, and let each part carry its own.
