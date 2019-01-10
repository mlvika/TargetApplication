package com.target.usecase.model;

import java.io.IOException;
import java.io.Serializable;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

public class DataPlatformEvent implements Serializable
{
    private static final long serialVersionUID = 1L;
    protected static ObjectMapper _mapper = new ObjectMapper();

    private String _location;
    private Long _timestamp;
    private String _hostIp;
    private int _viewership;

    public DataPlatformEvent(String src, Long timestamp, String host_ip, String rawdata)
    {
        _location = location;
        _viewership = viewership;
        _timestamp = timestamp;
        _hostIp = host_ip;
        _rawdata = rawdata;
    }

    public String getLocation()
    {
        return _location;
    }

    public String getViewership()
    {
        return _viewership;
    }

    public Long getTimestamp()
    {
        return _timestamp;
    }

    public String getHostIp()
    {
        return _hostIp;
    }

    @Override
    public String toString()
    {
        try
        {
            return _mapper.writeValueAsString(this);
        }
        catch (Exception ex)
        {
            return null;
        }
    }

    @Override
    public boolean equals(Object other)
    {
        boolean result = false;
        if (other instanceof DataPlatformEvent)
        {
            DataPlatformEvent that = (DataPlatformEvent) other;
            result =   (this.getSrc()       == that.getSrc()       || (this.getSrc()       != null && this.getSrc().equals(that.getSrc())))
                    && (this.getTimestamp() == that.getTimestamp() || (this.getTimestamp() != null && this.getTimestamp().equals(that.getTimestamp())))
                    && (this.getHostIp()    == that.getHostIp()    || (this.getHostIp()    != null && this.getHostIp().equals(that.getHostIp())))
                    && (this.getRawdata()   == that.getRawdata()   || (this.getRawdata()  != null && this.getRawdata().equals(that.getRawdata())));
        }
        return result;

    }

}
