package fr.insalyon.creatis.gasw.executor.batch.internals.terminal;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * If you want to copy a file from local to remote you do:
 * source: localpath/file.txt
 * dest: remotepath/remote.txt
 * 
 * If you want to do in reverse:
 * source: remotepath/remote.txt
 * dest: localpath/file.txt
 */
@RequiredArgsConstructor
@Getter
public class RemoteFile {

    private final String source;
    private final String dest;
}
