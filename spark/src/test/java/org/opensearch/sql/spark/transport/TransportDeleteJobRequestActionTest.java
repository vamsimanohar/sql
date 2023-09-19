/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;

import java.util.HashSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.spark.jobs.JobExecutorServiceImpl;
import org.opensearch.sql.spark.transport.model.DeleteJobActionRequest;
import org.opensearch.sql.spark.transport.model.DeleteJobActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportDeleteJobRequestActionTest {

  @Mock private TransportService transportService;
  @Mock private JobExecutorServiceImpl jobExecutorService;
  @Mock private TransportDeleteJobRequestAction action;
  @Mock private Task task;
  @Mock private ActionListener<DeleteJobActionResponse> actionListener;
  @Captor private ArgumentCaptor<DeleteJobActionResponse> deleteJobActionResponseArgumentCaptor;
  @Captor private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportDeleteJobRequestAction(
            transportService, new ActionFilters(new HashSet<>()), jobExecutorService);
  }

  @Test
  public void testDoExecute() {
    DeleteJobActionRequest request = new DeleteJobActionRequest(EMR_JOB_ID);
    when(jobExecutorService.cancelJob(EMR_JOB_ID)).thenReturn(EMR_JOB_ID);
    action.doExecute(task, request, actionListener);
    Mockito.verify(actionListener).onResponse(deleteJobActionResponseArgumentCaptor.capture());
    DeleteJobActionResponse deleteJobActionResponse =
        deleteJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals(
        "Deleted job with id: " + EMR_JOB_ID, deleteJobActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithException() {
    DeleteJobActionRequest request = new DeleteJobActionRequest(EMR_JOB_ID);
    doThrow(new RuntimeException("Error")).when(jobExecutorService).cancelJob(EMR_JOB_ID);
    action.doExecute(task, request, actionListener);
    Mockito.verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("Error", exception.getMessage());
  }
}
