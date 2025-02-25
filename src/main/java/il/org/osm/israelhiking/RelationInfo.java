package il.org.osm.israelhiking;

import java.util.List;

import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;

// Minimal container for data we extract from OSM route relations. This is held in RAM so keep it small.
public class RelationInfo implements OsmRelationInfo {

    long _id;

    RelationInfo(long id) {
        this._id = id;
    }

    PointDocument pointDocument;

    // OSM ID of the relation (required):
    @Override
    public long id() { 
        return this._id;
    }

    Long firstMemberId;
    Long secondMemberId;
    List<Long> memberIds;
    SourceFeature firstMemberFeature;
    SourceFeature secondMemberFeature;
}