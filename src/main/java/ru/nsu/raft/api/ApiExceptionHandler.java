package ru.nsu.raft.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.concurrent.TimeoutException;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(NotLeaderException.class)
    public ResponseEntity<ApiError> notLeader(NotLeaderException error) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(new ApiError(error.getMessage(), error.leaderId()));
    }

    @ExceptionHandler(TimeoutException.class)
    public ResponseEntity<ApiError> timeout() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(new ApiError("Request timed out; the entry may still commit later", null));
    }

    @ExceptionHandler({IllegalArgumentException.class, HttpMessageNotReadableException.class})
    public ResponseEntity<ApiError> invalidRequest() {
        return ResponseEntity.badRequest().body(new ApiError("Invalid request body", null));
    }

    public record ApiError(String message, String leaderId) {
    }
}
