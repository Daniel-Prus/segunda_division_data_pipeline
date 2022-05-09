from seaborn import light_palette


def draw_performance_visual(dataframe, title='', hide_col=None):
    """Provide table visualization with pandas style.

        Parameters
        ----------
        dataframe (pd.DataFrame): table.
        title (str): table title.
        hide_col (list): Default - None. List(int) integer position of columns to hide.

        Returns
        ----------
        pandas.io.formats.style.Styler: visualized dataframe.
    """

    columns_to_hide = []

    if hide_col is not None:
        for col in hide_col:
            columns_to_hide.append(dataframe.columns[col])

    green_background_col = []
    red_bar_col = []
    red_background_col = []
    green_bar_col = []
    format_dict = {}

    for column in dataframe.columns:
        if 'draws_pcts' in column:
            green_background_col.append(column)
            format_dict.update({column: "{:.2f}"})
        elif 'no_draw_max' in column:
            red_bar_col.append(column)
        elif 'no_draw_mean' in column:
            red_background_col.append(column)
            format_dict.update({column: "{:.2f}"})
        elif 'draw_mean' in column:
            green_background_col.append(column)
            format_dict.update({column: "{:.2f}"})
        elif 'draw_max' in column:
            green_bar_col.append(column)
        elif 'goals_mean' in column:
            format_dict.update({column: "{:.2f}"})

    index_names = {
        'selector': '.index_name',
        'props': [('font-style', 'italic'), ('color', 'black'), ('font-weight', 'bolder'),
                  ('background-color', '#f0ffff')]}

    headers = {
        'selector': 'th:not(.index_name)',
        'props': [('background-color', '#000066'), ('color', 'white'), ('border', 'outset'), ('border-color', 'white'),
                  ('border-width', '0.001em')]}

    caption = {
        'selector': 'caption',
        'props': [('color', '#2F4F4F'), ('font-size', '14px'), ('font-weight', 'bolder'), ('font-style', 'italic')]}

    draw_perf_visual = (dataframe.style
                        .format(format_dict)
                        .background_gradient(subset=green_background_col, cmap=light_palette("seagreen", as_cmap=True))
                        .background_gradient(subset=red_background_col, cmap=light_palette("indianred", as_cmap=True))
                        .bar(subset=red_bar_col, align='left', color="indianred")
                        .bar(subset=green_bar_col, align='left', color='seagreen')
                        .set_caption(title)
                        .set_table_styles([caption, index_names, headers])
                        .hide_columns(columns_to_hide))

    return draw_perf_visual
