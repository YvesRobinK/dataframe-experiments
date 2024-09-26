class IOParser():
    def __init__(self,
                 filename: str = "/proc/net/dev"
            ):
        self.filename = filename

    def set_up(self):
        with open(self.filename) as f:
            self.before_download = f.readlines()

    def get_result(self) -> (int, int):
        with open(self.filename) as f:
            self.after_download = f.readlines()
        return self.parse_files()

    def parse_files(self) -> (int, int):
        self.header = self.before_download[1]
        bf_lines, a_lines = self.extract_network_line()
        sum_received_before = 0
        sum_sent_before = 0
        sum_received_after = 0
        sum_sent_after = 0
        for index in range (len(bf_lines)):

            sum_received_before += int(bf_lines[index][1])
            sum_sent_before += int(bf_lines[index][9])
            sum_received_after += int(a_lines[index][1])
            sum_sent_after += int(a_lines[index][9])
        return (sum_received_after - sum_received_before, sum_sent_after - sum_sent_before)

    def extract_network_line(self) -> ([[str]],[[str]]):
        before_network_lines = []
        after_network_lines = []
        for network_lines in self.before_download[2:]:
            before_network_lines.append(network_lines.split())
        for network_lines in self.after_download[2:]:
            after_network_lines.append(network_lines.split())
        return (before_network_lines, after_network_lines)





