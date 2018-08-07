import pandas as pd
from pandas.core.base import DataError


class WithConsole:
    def __init__(self, *args, **kwargs):
        self._console = kwargs.pop('console', None)
        super().__init__(*args, **kwargs)

    def cls(self):
        if self._console is not None:
            self._console.text = ""

    def log(self, message):
        if self._console is not None:
            self._console.text += message + '\n'
            self._console.text = '\n'.join(self._console.text.split('\n')[-20:])


class PrepareDataMixin:
    def __init__(self, *args, **kwargs):
        self._time_field = kwargs.pop('time_field', None)
        self._columns = kwargs.pop('columns', None)
        self._time_unit = kwargs.pop('time_unit', None)
        super().__init__(*args, **kwargs)


    def _prepare_data(self, data, options=None):
        """
        Returns a cleaned up unique-key dataframe according to _columns spec
        :param data:
        :return:
        """
        if options is None:
            options = dict(
                time_field=getattr(self, '_time_field', 'time'),
                columns=getattr(self, '_columns', ['defaultcolumn']),
                time_unit=getattr(self, '_time_unit', 's'),
            )
        if isinstance(data, pd.Series):
            data = [data]
        df = pd.DataFrame(data, columns=options['columns']).groupby(options['time_field'])
        columns = []
        for column in options['columns']:
            if column != 'volume':
                try:
                    series = df[column].mean()
                except DataError as e:
                    series = df[column].first()
            else:
                series = df[column].sum()
            columns.append(series)
        df = pd.concat(columns, axis=1)
        df['Time'] = pd.to_datetime(df[options['time_field']], unit=options['time_unit'])
        df.set_index('Time', inplace=True)
        del data, columns
        df.sort_index(inplace=True)
        return df
