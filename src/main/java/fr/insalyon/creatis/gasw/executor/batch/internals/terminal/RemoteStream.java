package fr.insalyon.creatis.gasw.executor.batch.internals.terminal;

import fr.insalyon.creatis.gasw.executor.batch.config.Constants;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class RemoteStream {

    final String content;

    public String[] getLines() {
        return content.split("\n");
    }

    /**
     * @return the splited row(index)
     */
    public String[] getRow(final int index) {
        final String[] splited = getLines();

        if (index >= splited.length) {
            throw new IndexOutOfBoundsException();
        }
        return splited[index].split(Constants.SPLIT_REGEX);
    }

    /**
     * We assume that all rows have the same format !
     * @return the splited column(index)
     */
    public String[] getColumn(final int index) {
        final String[] first = getRow(0);
        final String[] result = new String[getLines().length];

        if (index > first.length) {
            throw new IndexOutOfBoundsException();
        }
        for (int i = 0; i < getLines().length; i++) {
            result[i] = getRow(i)[index];
        }
        return result;
    }
}
