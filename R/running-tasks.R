library(ggplot2)
file <- "" # Add location of file created by RunningTasks Java class
data <- read.csv(file, header=TRUE)
ggplot(data, aes(x=TIME,y=RUNNING_TASKS)) +
  xlab("Time in seconds") +
  ylab("Running tasks") +
  geom_line( color="#69b3a2", size=0.7, linetype=1) +
  theme(axis.text=element_text(size=12),
        axis.title=element_text(size=14,face="bold"))
ggsave("./running-tasks.jpg")
