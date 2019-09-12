package com.aiplus.bi.alarm.resource;

import com.aiplus.bi.alarm.AlarmConfigurable;
import com.aiplus.bi.alarm.Configuration;
import com.aiplus.bi.alarm.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * @author dev
 */
@Path("alarm")
public class AlarmResource implements AlarmConfigurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlarmResponse.class);

    @Inject
    private Configuration conf;

    @GET
    @Path("info")
    @Produces(MediaType.APPLICATION_JSON)
    public Response info() {
        return Response.ok().entity("OK!It works!").build();
    }

    @GET
    @Path("conf")
    @Produces(MediaType.APPLICATION_JSON)
    public Response conf() {
        return Response.ok().entity(conf.all()).build();
    }

    @POST
    @Path("{alarmType}/send")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response send(@PathParam("alarmType") String alarmType, AlarmRequest request) {
        conf.set(AlarmConfigurable.ALARM_ENGINE_TYPE, alarmType);
        AlarmEngine engine = AlarmEngineFactory.getInstance().createEngine(conf);
        try {
            AlarmResponse response = engine.accept(request).execute();
            return Response.ok().entity(response).build();
        } catch (AlarmException e) {
            LOGGER.error("Alarm engine process has some exception.", e);
            return Response.serverError().entity("Alarm engine process has some exception.").build();
        }
    }
}
