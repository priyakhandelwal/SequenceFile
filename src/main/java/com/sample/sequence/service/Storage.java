package com.sample.sequence.service;

import java.io.IOException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;
import com.sample.sequence.utils.SequenceFileAppend;
import com.sample.sequence.utils.SequenceFileReader;
import com.sample.sequence.utils.Create;

/**
 * 
 * @author prkhandelwal
 *	Sample class file to append content to an existing sequence file
 */

@Path("/sample")
public class Storage {
	@POST
	@Path("/create")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response createFileInSequence(Create create) {
		SequenceFileAppend sequenceFileAppend = new SequenceFileAppend();
		String result;
		try {
			sequenceFileAppend.createfile(create);
			result = create.getPathName() + "is created";
			return Response.status(201).entity(result).build();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(500).entity(e.getStackTrace()).build();
		}
	};
	
	@GET
	@Path("/get/{filename}")
	@Produces(MediaType.TEXT_PLAIN)
	public Response getFileContent(@PathParam("filename") String filename) {
		System.out.println("filename = " + filename);
		SequenceFileReader sequenceFileReader = new SequenceFileReader(); 
		String result = sequenceFileReader.getContent(filename);
		
		if (result != null) {
			System.out.println("result = " + result);
			return Response.status(200).entity(result).build();
		} else {
			return Response.status(404).entity("Not found").build();
		}
		
	}
	
	@GET
	@Path("/test")
	public Response test() {
		return Response.status(200).entity("Yay gotcha!").build();
	}
	
	@GET
    @Path("/intextform")
    @Produces(MediaType.TEXT_PLAIN)
    public String helloWorld(){
        return "Hello from Jersey!";

       }
}
