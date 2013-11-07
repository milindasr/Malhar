package com.datatorrent.contrib.couchdb;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * Implementation of {@link AbstractCouchDBOutputOperator}  that saves a Map in the couch database<br></br>
 *
 * @since 0.3.5
 */
public class MapBasedCouchDbOutputOperator extends AbstractCouchDBOutputOperator<Map<Object, Object>>
{

  @Override
  public CouchDbTuple getCouchDbTuple(Map<Object, Object> tuple)
  {
    return new MapBasedCouchTuple(tuple);
  }

  public static class MapBasedCouchTuple implements CouchDbTuple
  {

    private final Map<Object, Object> payload;

    private MapBasedCouchTuple()
    {
      this.payload = null;
    }

    public MapBasedCouchTuple(Map<Object, Object> payload)
    {
      this.payload = Preconditions.checkNotNull(payload, "payload");
    }

    @Nullable
    @Override
    public String getId()
    {
      return (String) payload.get("_id");
    }

    @Nullable
    @Override
    public String getRevision()
    {
      return (String) payload.get("_rev");
    }

    @Nonnull
    @Override
    public Map<Object, Object> getPayLoad()
    {
      return payload;
    }

    @Override
    public void setRevision(String revision)
    {
      payload.put("_rev", revision);
    }

    @Override
    public void setId(String id)
    {
      payload.put("_id", id);
    }
  }
}