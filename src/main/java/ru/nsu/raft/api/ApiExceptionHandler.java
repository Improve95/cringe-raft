package ru.nsu.raft.api;

import java.util.concurrent.TimeoutException;
import org.springframework.http.*;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.*;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(NotLeaderException.class)
    public ResponseEntity<ApiError> notLeader(NotLeaderException exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ApiError(exception.getMessage(), exception.leaderId()));
    }
    @ExceptionHandler(TimeoutException.class)
    public ResponseEntity<ApiError> timeout() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ApiError("Confirmation timed out; entry may still be committed", null));
    }
    @ExceptionHandler({IllegalArgumentException.class, HttpMessageNotReadableException.class})
    public ResponseEntity<ApiError> invalidRequest(Exception exception) {
        return ResponseEntity.badRequest().body(new ApiError("Invalid request", null));
    }
    public record ApiError(String message, String leaderId) {}
}
