package org.wyh.db.utils;

import org.neo4j.graphdb.RelationshipType;

public enum RelTypes implements RelationshipType {
	IS_FRIEND_OF,
	CHECKIN;
}
